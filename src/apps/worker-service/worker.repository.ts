import { Inject, Injectable, Logger } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../../shared/database/database.service';
import { RABBIT_CONFIG, type RabbitConfig } from '../../shared/rabbit/rabbit.config';
import { elapsedMs, startTimer } from '../../shared/observability/timing';
import { type OpportunityRef } from '../../shared/filter/snapshot-query';

/** What the worker tells the broker about a batch. */
export type BatchDisposition = 'applied' | 'skipped' | 'dead' | 'retry';

/**
 * Where the wall clock went on a batch.
 *
 * These are measured rather than derived, because the split is invisible in the
 * data. started_at is stamped after the claim lands, so the time between the
 * broker handing a message to a consumer and that stamp - which is exactly where
 * a saturated connection pool shows up - leaves no trace in any column. The two
 * halves also fail for unrelated reasons: pool wait is a capacity problem,
 * workMs is a contention or query problem, and the fix for each is different.
 *
 * poolSize and poolWaiting are sampled at the moment the connection is taken,
 * not accumulated, so they describe the pressure this batch actually met.
 */
export interface BatchTiming {
  /** Waiting for a free connection. Zero when one was already available. */
  poolWaitMs: number;
  /** Claim and apply, measured on the connection once it was held. */
  workMs: number;
  /** Consumers queued for a connection when this one was taken. */
  poolWaiting: number;
  /** Connections the pool is holding, against PG_POOL_MAX. */
  poolSize: number;
}

export interface BatchResult {
  disposition: BatchDisposition;
  /** Records this batch actually moved. */
  moved: number;
  /** Records this batch gave up on, each with its own reason. */
  failed: number;
  /**
   * Records that had left the filtered set by the time the batch reached them.
   *
   * Reported rather than folded into either of the others, because it is the
   * number that explains why a job's processed and failed counts do not add up to
   * its match count. Without it the difference looks like lost work.
   */
  skipped: number;
  attempts: number;
  reason?: string;
  /** Absent only on paths that never reached a claim. */
  timing?: BatchTiming;
}

interface JobSpec {
  target_stage_id: string;
  /** The filter's stage list, or null when the filter did not name stages. */
  filter_stage_ids: string[] | null;
  /**
   * The job's watermark, as timestamptz text rather than a timestamptz.
   *
   * Text because this value is compared against `stage_decided_at` by other jobs
   * at full microsecond precision, and a JS Date holds milliseconds. Read as a
   * timestamptz it would be truncated here and every comparison would be made
   * against a value a fraction of a millisecond below the truth.
   *
   * snapshot_at is written once, at insert, and never updated, so one read serves
   * both uses below.
   */
  snapshot_at: string;
}

/**
 * All SQL the batch worker runs.
 *
 * A batch is a position, not a stored set. Batch N is the Nth page of the job's
 * filter evaluated against its watermark, so nothing had to be written per record
 * at submission and the worker re-derives the same page on every attempt.
 *
 * Two consequences worth stating, because they are the design and not an
 * accident:
 *
 * Retrying re-examines the whole page rather than only what is outstanding, so
 * the transition insert needs UNIQUE (job_id, opportunity_id) and ON CONFLICT
 * DO NOTHING to stay idempotent - otherwise a redelivered batch would write a
 * second audit row for every record it already moved.
 *
 * And there is no per-record status, so a record that a user moved by hand
 * between submission and processing is simply no longer in the page. It is not
 * overwritten and not reported as a failure; it was out of scope by the time the
 * job reached it. That is the trade that removed 50,000 rows per job, and it is
 * why total_matched means "matched at submission".
 */
@Injectable()
export class WorkerRepository {
  private readonly logger = new Logger(WorkerRepository.name);

  constructor(
    private readonly db: DatabaseService,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  /**
   * Processes one batch, entirely on one dedicated connection.
   *
   * The connection is held for the whole call because two things bind to the
   * session rather than the transaction. The advisory lock that keeps a second
   * worker off the batch, and the attempt counter, which must commit before the
   * work runs - incremented inside the work transaction, a rollback would erase
   * it and the batch would retry for ever rather than ever being marked failed.
   *
   * Taking the lock through the pool instead would put it on an arbitrary
   * connection: a second worker could then acquire the same key on a different
   * connection, and the first connection would return to the pool still holding
   * it, so that batch could never be locked again and its job would hang.
   *
   * Postgres drops the advisory lock when the connection dies, so a killed
   * worker leaves nothing stranded.
   */
  async processBatch(workspaceId: string, jobId: string, batchNo: number): Promise<BatchResult> {
    // Timed here rather than inside the work, because the wait for a connection
    // is the one part of a batch that no column records. The claim stamps
    // started_at after it lands, so by the time any row knows this batch was
    // touched, whatever it spent queuing for a connection is already gone.
    //
    // connect() is exactly the pool wait and nothing else - pg resolves it only
    // once a client is free - so this is a measurement, not an estimate.
    const waited = startTimer();
    const client = await this.db.connect();
    const poolWaitMs = elapsedMs(waited);
    const poolWaiting = this.db.pool.waitingCount;
    const poolSize = this.db.pool.totalCount;

    const worked = startTimer();
    const result = await this.claimAndRun(client, workspaceId, jobId, batchNo);
    return {
      ...result,
      timing: { poolWaitMs, workMs: elapsedMs(worked), poolWaiting, poolSize },
    };
  }

  /**
   * Takes the batch's advisory lock and applies it.
   *
   * Split out of processBatch purely so the timing wrapper above has something
   * to measure; the behaviour and the connection handling are unchanged, and the
   * long comment on the lock belongs to this half.
   */
  private async claimAndRun(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
  ): Promise<BatchResult> {
    const key = `${jobId}:${batchNo}`;
    // If the unlock below fails, this connection still holds a session-scoped
    // advisory lock. Releasing it back to the pool would leak that key for the
    // life of the process and no worker could ever lock that batch again, so the
    // connection is destroyed instead. Destroying it closes the socket, and
    // Postgres drops the lock with it.
    let destroy = false;
    try {
      const lock = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
        [key],
      );
      if (!lock.rows[0]?.locked) {
        return { disposition: 'skipped', moved: 0, failed: 0, skipped: 0, attempts: 0 };
      }
      try {
        return await this.runOn(client, workspaceId, jobId, batchNo);
      } finally {
        try {
          await client.query('SELECT pg_advisory_unlock(hashtext($1))', [key]);
        } catch {
          destroy = true;
        }
      }
    } finally {
      client.release(destroy);
    }
  }

  private async runOn(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
  ): Promise<BatchResult> {
    // --- attempt counter, committed on its own so a rollback cannot erase it
    //
    // The predicate takes 'running' as well as 'pending', exactly as the old
    // per-item claim did. A batch whose worker died mid-flight has to be
    // re-claimable, or the redelivered message would find nothing pending and
    // the job would hang. The advisory lock is what makes re-claiming safe: one
    // worker is ever inside here for a batch. A batch that already committed is
    // 'completed' and matches neither, so redelivery does nothing.
    // The rollback matters: without it a failure here leaves an open, aborted
    // transaction on a session-scoped connection. The next statement on it fails
    // too, the connection returns to the pool still dirty, and every later user
    // of that pooled connection inherits the wreckage. Observed exactly that way
    // when this statement named a column that did not exist: four workers sat on
    // `idle in transaction (aborted)` and the batches looped through the retry
    // queue for ever, because the failure never reached the attempt counter.
    await client.query('BEGIN');
    let claimed;
    try {
      // completed_count and failed_count come back too, and they are what makes
      // the job counters an increment rather than a sum. They hold whatever this
      // batch last settled - zero on a first attempt, its previous totals on a
      // retry - so the delta can be applied instead of the absolute count.
      claimed = await client.query<{
        attempts: number;
        completed_count: number;
        failed_count: number;
      }>(
        `UPDATE bulk_job_outbox
            SET status = 'running', attempts = attempts + 1,
                started_at = COALESCE(started_at, now()), updated_at = now()
          WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3
            AND status IN ('pending', 'running')
          RETURNING attempts, completed_count, failed_count`,
        [jobId, batchNo, workspaceId],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    }

    if (claimed.rows.length === 0) {
      return { disposition: 'skipped', moved: 0, failed: 0, skipped: 0, attempts: 0 };
    }
    const attempts = claimed.rows[0]!.attempts;
    const prior = {
      completed: claimed.rows[0]!.completed_count,
      failed: claimed.rows[0]!.failed_count,
    };

    if (attempts > this.config.maxAttempts) {
      const failed = await this.failBatch(
        client,
        workspaceId,
        jobId,
        batchNo,
        'attempts exhausted',
        prior.failed,
      );
      return {
        disposition: 'dead',
        moved: 0,
        failed,
        skipped: 0,
        attempts,
        reason: `${failed} record(s) failed`,
      };
    }

    // --- the work, one transaction. Per-record outcomes are committed together,
    // so a batch never leaves half its records moved and half not.
    await client.query('BEGIN');
    try {
      const result = await this.applyOn(client, workspaceId, jobId, batchNo, attempts, prior);
      await client.query('COMMIT');
      return { disposition: 'applied', ...result, attempts };
    } catch (error) {
      await client.query('ROLLBACK');
      const reason = error instanceof Error ? error.message : 'unexpected worker failure';
      // Only reached for conditions that are not per-record: a lost job row, or
      // a concurrent write. Those are worth retrying, and only the attempt that
      // spends the budget records the failure - reporting 'dead' without writing
      // it would ack the message and leave the batch running forever.
      if (attempts >= this.config.maxAttempts) {
        const failed = await this.failBatch(
          client,
          workspaceId,
          jobId,
          batchNo,
          reason,
          prior.failed,
        );
        return {
          disposition: 'dead',
          moved: 0,
          failed,
          skipped: 0,
          attempts,
          reason: `${failed} record(s) failed: ${reason}`,
        };
      }
      return { disposition: 'retry', moved: 0, failed: 0, skipped: 0, attempts, reason };
    }
  }

  /**
   * Applies the batch, one record at a time.
   *
   * The batch is the unit of dispatch and of retry, not of atomicity. A record
   * that cannot move fails on its own and the rest of the batch still lands.
   * Refusing the whole batch for one blocked record would mean a single
   * unmovable deal in 50,000 leaves 49,999 untouched, which is not a useful bulk
   * move.
   *
   * Four outcomes, and the distinction between the middle two is the whole point
   * of re-reading live state rather than trusting the stored membership:
   *
   *   moved     - in the filtered set, still there, and the move is permitted
   *   left      - a user moved it out of the filtered set since submission. It is
   *               not moved and not reported as an error, because undoing
   *               someone's deliberate change is worse than not finishing the
   *               job. This is the case the design trades a frozen-set guarantee
   *               for, and it is why processed_count can land below total_matched.
   *   unmovable - no transition rule permits the move from where it sits
   *   gone      - the record no longer exists
   *
   * Throws only for conditions that are not per-record: a missing job row, or a
   * concurrent write landing between the read and the write below. Both mean
   * there is no correct partial answer, so the transaction rolls back and the
   * batch is retried.
   */
  private async applyOn(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
    attempts: number,
    prior: { completed: number; failed: number },
  ): Promise<{ moved: number; failed: number; skipped: number }> {
    // The target and the batch's membership both come from the database, not the
    // message, so a hand-crafted message cannot redirect a batch or hand it
    // records it was not given. The filter's stage list comes along too, because
    // "has this record left the set the job was asked to move" is the question
    // that decides whether to touch it at all.
    //
    // The watermark comes along for the same reason, and this is now the only
    // read of bulk_job in this method. It used to be fetched twice more - a cross
    // join against bulk_job to reach snapshot_at for the live read below, and a
    // subquery inside the UPDATE - so the job row was read three times per batch
    // for one row of one column. It is read once here and the value is reused.
    const job = await client.query<JobSpec>(
      `SELECT target_stage_id, filter->'stageId' AS filter_stage_ids,
              snapshot_at::text AS snapshot_at
         FROM bulk_job WHERE id = $1 AND workspace_id = $2`,
      [jobId, workspaceId],
    );
    const spec = job.rows[0];
    if (!spec) throw new BatchRejected('job row is gone, so the target stage is unknown');
    const targetId = spec.target_stage_id;
    // Null means the filter did not name stages, so every record is in scope and
    // there is nothing to compare against.
    const filterStages = new Set<string>(spec.filter_stage_ids ?? []);

    const batch = await client.query<{ item_ids: string[] }>(
      `SELECT item_ids FROM bulk_job_outbox
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3`,
      [jobId, batchNo, workspaceId],
    );
    const ids = batch.rows[0]?.item_ids ?? [];
    if (ids.length === 0) {
      await this.settleBatch(client, jobId, batchNo, workspaceId, 0, 0);
      await this.settleJob(client, workspaceId, jobId);
      return { moved: 0, failed: 0, skipped: 0 };
    }

    // The live stage for every record in the batch. This is the baseline the
    // decision is made against - the stored membership says which records to
    // consider, and this says where they actually are now.
    //
    // decided_since is the logical clock, and it is computed here in SQL rather
    // than in the loop below for two reasons. The comparison is
    // stage_decided_at > snapshot_at done in the database, so the answer comes
    // back as a boolean and no timestamp has to be read into JS and compared
    // there. And it is free - this query already fetches per-record state.
    //
    // snapshot_at arrives as the lossless text read at the top of this method,
    // cast back to timestamptz so the comparison is column against value on the
    // same scale. The cross join against bulk_job this replaces existed only to
    // hand the column to the comparison; the value is the same one, already read.
    const live = await client.query<OpportunityRef & { decided_since: boolean }>(
      `SELECT o.id, o.stage_id, o.stage_decided_at > $3::timestamptz AS decided_since
         FROM opportunity o
        WHERE o.workspace_id = $1 AND o.id = ANY($2::uuid[])`,
      [workspaceId, ids, spec.snapshot_at],
    );
    const present = new Map(live.rows.map((r) => [r.id, r.stage_id]));
    // Read separately rather than folded into `present`, which is keyed by stage
    // and is asked "where is it". This one answers "has it changed since we were
    // submitted", which is a different question and the reason this column
    // exists - a record can sit in a stage the filter names and still have been
    // moved there deliberately since submission.
    const changedSince = new Set(live.rows.filter((r) => r.decided_since).map((r) => r.id));

    // One query for the rules of every stage this batch starts from, not 1000.
    const rules = await client.query<{ from_stage_id: string; to_stage_id: string }>(
      `SELECT from_stage_id, to_stage_id FROM stage_transition_rule
        WHERE workspace_id = $1 AND from_stage_id = ANY($2::uuid[])`,
      [workspaceId, [...new Set(present.values())]],
    );
    const permitted = new Set(rules.rows.map((r) => `${r.from_stage_id}->${r.to_stage_id}`));

    const movable: { id: string; stage_id: string }[] = [];
    // Keyed by reason so each distinct cause is recorded on its own records.
    const failures = new Map<string, string[]>();
    const fail = (reason: string, id: string): void => {
      const list = failures.get(reason);
      if (list) list.push(id);
      else failures.set(reason, [id]);
    };

    // Iterate the stored membership, not what came back. A record that has since
    // been deleted is in neither set, and dropping it silently would hide a real
    // loss; a record that has left the filtered set is in one and must be
    // recognised as out of scope rather than moved.
    //
    // The order of these checks is the design, not an accident.
    let alreadyThere = 0;
    let leftScope = 0;
    let changedAfterSubmit = 0;
    for (const id of ids) {
      const stageId = present.get(id);
      if (stageId === undefined) {
        fail('opportunity no longer exists', id);
        continue;
      }
      if (stageId === targetId) {
        // Already where the job wanted it. Counted as moved rather than failed,
        // because on a retry that is exactly what it is - the previous attempt
        // did this record before dying. Treating it as a failure would turn every
        // successful record into a failure on the second attempt.
        //
        // Ahead of the clock check below, and it has to stay there. A person who
        // moves a record *to* the target has done what the job wanted, and their
        // stamp is newer than the job's, so checking the clock first would report
        // that as a skip and under-report a job that in fact achieved its intent.
        alreadyThere += 1;
        continue;
      }
      if (changedSince.has(id)) {
        // Somebody decided this record's stage after this job was submitted, and
        // that decision is newer. Left alone for the same reason as the check
        // below: undoing a deliberate change is worse than not finishing the job.
        //
        // This is the case the stored membership cannot see. The record may well
        // still be in a stage the filter names - the check below would pass it
        // through - and it is this one that stops a bulk job overwriting a
        // deliberate edit made while the job was still being built.
        changedAfterSubmit += 1;
        continue;
      }
      if (filterStages.size > 0 && !filterStages.has(stageId)) {
        // Moved out of the stages this job was asked to act on. Left alone.
        leftScope += 1;
        continue;
      }
      if (!permitted.has(`${stageId}->${targetId}`)) {
        fail('no permitted transition to the target stage', id);
        continue;
      }
      movable.push({ id, stage_id: stageId });
    }

    // Compare-and-swap per record, pairing each id with the stage it was read in.
    // Comparing against a set of stages would let a record that had been moved to
    // a *different* member of that set slip through.
    let movedCount = 0;
    if (movable.length > 0) {
      // One statement, not two. The move and its audit row are written by a single
      // data-modifying CTE, so the transition can only exist for a record this
      // statement actually moved.
      //
      // That is a stronger guarantee than the two statements it replaces, and the
      // difference is the RETURNING. Previously the insert was driven by the
      // `movable` array computed in JS, and the reason a record could not be
      // inserted twice was an argument made in this file: a record already at the
      // target was left out of `movable` when the batch was classified. Correct,
      // but it is a property of this code being right rather than of the database
      // enforcing anything - and there is no unique key on
      // (job_id, opportunity_id) to catch it if the argument were ever wrong. Now
      // the rows to audit are the rows the UPDATE returned, so the database
      // guarantees it and the argument is not load-bearing.
      //
      // Worth one round trip on its own: this runs per batch, and the batch is
      // already the unit of work and of retry, so halving the statements on the
      // hot path is not nothing at 12 consumers.
      //
      // stage_decided_at is set to this job's own snapshot_at, not now(). That
      // makes the column a logical clock saying whose decision put the record
      // here, so a job that arrives later but was submitted earlier still defers -
      // writing the wall clock instead would make this last-writer-wins and let an
      // older job still draining beat a newer one.
      //
      // Bound as timestamptz text, read once at the top of this method, rather
      // than fetched again by a subquery. The subquery this replaces was not
      // about the value - assigning the column is what stands the stage_decided_at
      // trigger down, and an assignment does that whether the right side is a
      // subquery or a parameter. It was about the timestamp crossing the wire: a
      // JS Date holds milliseconds where this column holds microseconds, so
      // binding one would stamp these records a fraction of a millisecond below
      // the truth, and a job submitted inside that fraction would read them as
      // unchanged since submission and move records it should have skipped.
      const moved = await client.query<{ id: string }>(
        `WITH moved AS (
           UPDATE opportunity o
              SET stage_id = $2, updated_at = now(),
                  stage_decided_at = $6::timestamptz
             FROM unnest($3::uuid[], $4::uuid[]) AS t(id, from_stage)
            WHERE o.workspace_id = $1 AND o.id = t.id AND o.stage_id = t.from_stage
           RETURNING o.id, o.stage_id AS previous_stage
         )
         INSERT INTO opportunity_transition
           (workspace_id, opportunity_id, from_stage_id, to_stage_id, job_id)
         SELECT $1, m.id, m.previous_stage, $2, $5 FROM moved m
         RETURNING opportunity_id AS id`,
        [
          workspaceId,
          targetId,
          movable.map((m) => m.id),
          movable.map((m) => m.stage_id),
          jobId,
          spec.snapshot_at,
        ],
      );
      movedCount = moved.rows.length;
      if (movedCount !== movable.length) {
        // Someone wrote between the read above and this update. Rolling back and
        // retrying is the only honest answer: the next attempt re-reads and will
        // classify those records correctly.
        throw new BatchRejected(
          `${movable.length - movedCount} record(s) changed stage during the batch`,
        );
      }
    }

    // Records already at the target count as this batch's work, whether this
    // attempt moved them or an earlier one did. Records that left scope count as
    // neither: they were in the match at submission and are not any more, which
    // is the documented reason processed_count can land below total_matched.
    const settled = movedCount + alreadyThere;
    const failedCount = [...failures.values()].reduce((a, l) => a + l.length, 0);

    for (const [reason, failedIds] of failures) {
      await client.query(
        `INSERT INTO bulk_job_failure
           (workspace_id, job_id, batch_no, opportunity_id, from_stage_id, error, attempts)
         SELECT $1, $2, $3, t.id, t.from_stage, $5, $6
           FROM unnest($4::uuid[], $7::uuid[]) AS t(id, from_stage)
         ON CONFLICT (job_id, opportunity_id) DO UPDATE
           SET attempts = bulk_job_failure.attempts + 1, error = EXCLUDED.error`,
        [
          workspaceId,
          jobId,
          batchNo,
          failedIds,
          reason.slice(0, 500),
          attempts,
          // A deleted record has no live stage, so its from_stage is null. The
          // column allows it for exactly this case.
          failedIds.map((id) => present.get(id) ?? null),
        ],
      );
    }

    await this.settleBatch(client, jobId, batchNo, workspaceId, settled, failedCount, [
      ...failures.keys(),
    ]);

    // The job's record counters take a delta, not an absolute and not a sum.
    //
    // A plain increment double-counts, because a retry settles the same batch
    // twice - the claim takes 'running' as well as 'pending' precisely so a dead
    // worker's batch can be re-taken. Five records run twice reported 10.
    //
    // Summing the batch rows was tried instead, and is wrong for a reason worth
    // recording: every worker updates this same row, so they serialise on it, and
    // a statement that blocks on a row lock has already taken its snapshot from
    // before the worker ahead of it committed. Its sum is stale by one batch, and
    // because it commits last the stale value is the one that survives. Observed
    // as processed_count of 49,000 on a 50,000 record job whose fifty batch rows
    // were all correct at 50,000.
    //
    // A delta is the way out: settled minus what this batch last contributed.
    // Additions commute, so the order workers commit in stops mattering, and a
    // retry that settles the same totals again contributes zero.
    await client.query(
      `UPDATE bulk_job
          SET processed_count = processed_count + $3,
              failed_count = failed_count + $4,
              status = CASE WHEN status = 'pending' THEN 'running' ELSE status END,
              started_at = COALESCE(started_at, now()),
              error = CASE WHEN $4 > 0 THEN $5::text ELSE error END,
              updated_at = now()
        WHERE id = $1 AND workspace_id = $2`,
      [
        jobId,
        workspaceId,
        settled - prior.completed,
        failedCount - prior.failed,
        [...failures.keys()].join('; ').slice(0, 500) || null,
      ],
    );

    await this.settleJob(client, workspaceId, jobId);
    // Both skip causes are reported as one count, because from the job's point of
    // view they are the same event: a record the job declined to touch. They are
    // counted apart above only so this line can say which was more common, and
    // because a record skipped for the clock is a materially different thing from
    // one skipped for scope - the first was a deliberate edit to a record that was
    // still in scope, and a user watching processed_count come in under
    // total_matched deserves to know that is why.
    if (changedAfterSubmit > 0) {
      this.logger.log(
        `job ${jobId} batch ${batchNo}: ${changedAfterSubmit} record(s) left alone, ` +
          `stage decided after this job was submitted`,
      );
    }
    return { moved: settled, failed: failedCount, skipped: leftScope + changedAfterSubmit };
  }

  /** Records what became of the batch, on the batch row. */
  private async settleBatch(
    client: PoolClient,
    jobId: string,
    batchNo: number,
    workspaceId: string,
    completed: number,
    failed: number,
    reasons: string[] = [],
  ): Promise<void> {
    await client.query(
      `UPDATE bulk_job_outbox
          SET status = 'completed',
              completed_count = $4,
              failed_count = $5,
              error = $6::text,
              completed_at = now(),
              updated_at = now()
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3`,
      [jobId, batchNo, workspaceId, completed, failed, reasons.join('; ').slice(0, 500) || null],
    );
  }

  /**
   * Marks a batch permanently failed. Only reached once the attempt limit is
   * spent, so it is the one place a batch gives up rather than settling.
   */
  private async failBatch(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
    reason: string,
    priorFailed: number,
  ): Promise<number> {
    await client.query('BEGIN');
    try {
      const size = await client.query<{ n: number }>(
        'SELECT cardinality(item_ids)::int AS n FROM bulk_job_outbox WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3',
        [jobId, batchNo, workspaceId],
      );
      const failed = size.rows[0]?.n ?? 0;
      await client.query(
        `UPDATE bulk_job_outbox
            SET status = 'failed', failed_count = $4, error = $5::text,
                completed_at = now(), updated_at = now()
          WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3`,
        [jobId, batchNo, workspaceId, failed, reason.slice(0, 500)],
      );
      // Delta, not a sum, for the same reason as in applyOn: this path can be
      // reached on a later attempt than the one that counted the records, and a
      // sum read under a row lock is stale by whatever committed while it waited.
      await client.query(
        `UPDATE bulk_job
            SET failed_count = greatest(failed_count + $2, 0), error = $4::text, updated_at = now()
          WHERE id = $1 AND workspace_id = $3`,
        [jobId, failed - priorFailed, workspaceId, reason.slice(0, 500)],
      );
      await this.settleJob(client, workspaceId, jobId);
      await client.query('COMMIT');
      return failed;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }

  /**
   * Moves the job to a terminal state once nothing is left to do.
   *
   * A job that moved 980 of 1,000 has completed, not failed: the work finished
   * and the 20 exceptions are reported through failed_count and the failures
   * endpoint. Calling that 'failed' would make a 40,000-of-50,000 job read as a
   * total loss. Only a job where every record failed is 'failed'.
   *
   * The EXISTS is over 50 batch rows rather than 50,000 items, which is both far
   * cheaper and no longer a point where every finishing batch contends on the
   * same scan. It is still evaluated under the row lock as the update takes it,
   * so two workers finishing the last two batches cannot both conclude the job
   * is finished.
   */
  private async settleJob(client: PoolClient, workspaceId: string, jobId: string): Promise<void> {
    await client.query(
      `UPDATE bulk_job j
          SET status = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_outbox b
                              WHERE b.job_id = j.id AND b.status IN ('pending','running'))
                  THEN 'running'
                WHEN j.processed_count = 0 AND j.failed_count > 0 THEN 'failed'
                ELSE 'completed'
              END,
              completed_at = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_outbox b
                              WHERE b.job_id = j.id AND b.status IN ('pending','running'))
                  THEN NULL
                ELSE now()
              END,
              updated_at = now()
        WHERE j.id = $1 AND j.workspace_id = $2
          AND j.status NOT IN ('completed','failed','preparing')`,
      [jobId, workspaceId],
    );
  }
}

/** A batch that cannot be applied as it stands. Retried, then marked failed. */
export class BatchRejected extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BatchRejected';
  }
}
