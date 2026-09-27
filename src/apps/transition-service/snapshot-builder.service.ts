import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RABBIT_CONFIG, type RabbitConfig } from '../../shared/rabbit/rabbit.config';
import { elapsedMs, ms1, startTimer } from '../../shared/observability/timing';
import { pageMatching, parseStoredFilter, BATCH_SIZE } from '../../shared/filter/snapshot-query';

/**
 * Builds a job's batches after the response has been sent.
 *
 * Submission writes the job row and returns. The walk that turns a filter into
 * batches is the expensive half - 0.56s to 2.30s at 50,000 records - and none of
 * it has to happen before the caller gets an id.
 *
 * It is in-process rather than on the broker because the cursor on the job row
 * makes an interrupted build recoverable: the next sweep picks up where this one
 * stopped. A broker message would be more durable and would add a second thing
 * that can fail.
 *
 * Each batch is committed with the cursor that follows it, in one transaction.
 * That is what makes the cursor trustworthy - it can never claim progress the data
 * does not have. It costs one commit per batch, which no longer matters because
 * none of this is on anyone's critical path any more.
 */
@Injectable()
export class SnapshotBuilder implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SnapshotBuilder.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(
    private readonly db: DatabaseService,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.sweep();
    }, this.config.snapshotSweepIntervalMs);
    // Do not hold the process open purely for the sweep.
    this.timer.unref();
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
  }

  /**
   * Builds whatever is still preparing, one job at a time.
   *
   * Two different concurrency questions live here, and only one of them has a
   * limit that matters.
   *
   * Within a job, the walk cannot be parallelised at all. Page N+1's predicate
   * contains page N's last row as its keyset cursor, so there is no way to ask
   * for page 2 before page 1 has returned. One job, one walker, always.
   *
   * Across jobs there is no such coupling - each job has its own filter, its own
   * watermark and its own cursor - so building three jobs at once is not merely
   * safe but the only way to keep up when three arrive together. That is what the
   * advisory lock below arranges: it makes "one walker per job" a rule the
   * database enforces rather than a property of there happening to be one
   * replica.
   *
   * Without it, a second replica is *correct* and *wasteful*: the batch insert is
   * ON CONFLICT DO NOTHING and both walkers compute the same cursor, so the
   * stored result is right either way. But every page is read and written twice,
   * and the waste scales with the replica count. Correct enough to ship is not
   * the same as working.
   *
   * A sweep already running is left alone rather than queued, for the same
   * reason: a second concurrent sweep in one process would walk the same cursors
   * for no gain.
   */
  async sweep(): Promise<void> {
    if (this.running || this.stopped) return;
    this.running = true;
    try {
      const jobs = await this.db.query<{ id: string; workspace_id: string }>(
        `SELECT id, workspace_id FROM bulk_job
          WHERE status = 'preparing'
          ORDER BY created_at
          LIMIT $1`,
        [this.config.snapshotSweepLimit],
      );
      for (const job of jobs) {
        if (this.stopped) return;
        try {
          await this.withClaim(job.id, () => this.build(job.id, job.workspace_id));
        } catch (error) {
          // Left preparing on purpose. The next sweep retries from the cursor, and
          // a job that cannot be built is one an operator should see rather than
          // one that silently completed having moved nothing.
          this.logger.error(`snapshot build failed for job ${job.id}: ${(error as Error).message}`);
        }
      }
    } finally {
      this.running = false;
    }
  }

  /**
   * Runs fn while holding this job's build lock, or returns false if another
   * process already holds it.
   *
   * The same primitive the worker uses to keep two consumers off one batch, for
   * the same reason: it binds to the session rather than the transaction, so it
   * has to be taken on a connection held open for the duration, and it has to be
   * released on that same connection.
   *
   * Two properties make it the right tool here rather than a lease column. It
   * needs no expiry logic, which is where lease-based claims go wrong - a builder
   * that stalls long enough for its lease to expire gets a second walker, and the
   * lease is now describing a process that is still running. And Postgres drops
   * the lock when the connection dies, so a replica that is killed mid-build
   * releases its jobs immediately instead of holding them for a timeout.
   *
   * A skip is not logged. It is the expected outcome whenever another replica is
   * already building that job, which is every job on every tick while a build is
   * in flight - at a 250ms interval that would be several lines a second saying
   * nothing. A job that nobody is building shows up as status='preparing' with no
   * completion line, which is the signal worth having.
   */
  private async withClaim(jobId: string, fn: () => Promise<void>): Promise<boolean> {
    const client = await this.db.connect();
    // If the unlock fails this connection still holds the lock, and handing it
    // back to the pool would leak the key for the life of the process - no
    // builder could ever walk that job again. Destroying it closes the socket and
    // Postgres drops the lock with it.
    let destroy = false;
    try {
      const lock = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
        [jobId],
      );
      if (!lock.rows[0]?.locked) return false;
      try {
        await fn();
      } finally {
        try {
          await client.query('SELECT pg_advisory_unlock(hashtext($1))', [jobId]);
        } catch {
          destroy = true;
        }
      }
      return true;
    } finally {
      client.release(destroy);
    }
  }

  /**
   * Walks the job's filter into batches, resuming from its cursor if it has one.
   *
   * Reads a page, writes it as a batch and advances the cursor, then repeats. The
   * loop ends on a short page rather than an empty one, so a match count that is an
   * exact multiple of the page size does not cost a final empty round trip.
   */
  async build(jobId: string, workspaceId: string): Promise<void> {
    const started = startTimer();
    const spec = await this.db.query<{
      filter: unknown;
      snapshot_at: Date;
      snapshot_cursor: Date | null;
      snapshot_cursor_id: string | null;
      created_at: Date;
      correlation_id: string | null;
    }>(
      `SELECT filter, snapshot_at, snapshot_cursor, snapshot_cursor_id,
              created_at, correlation_id
         FROM bulk_job
        WHERE id = $1 AND workspace_id = $2
          -- Preparing, and only preparing. The claim normally guarantees this
          -- walk is the only one, but the lock is released *after* the job row is
          -- finalised, so there is a window where the previous builder has already
          -- set the status to pending and has not yet let go. A second builder
          -- that acquires the key in that window would otherwise start the whole
          -- walk again from batch 0 - every page read and every insert discarded
          -- by ON CONFLICT. Reading the status here turns that into a no-op.
          AND status = 'preparing'`,
      [jobId, workspaceId],
    );
    const job = spec[0];
    if (!job) return;

    const filter = parseStoredFilter(job.filter);
    const query = <T>(sql: string, params: unknown[]): Promise<T[]> =>
      this.db.query<T>(sql, params);

    let cursor: { createdAt: Date; id: string } | null =
      job.snapshot_cursor && job.snapshot_cursor_id
        ? { createdAt: job.snapshot_cursor, id: job.snapshot_cursor_id }
        : null;
    let batchNo = 0;
    let matched = 0;
    // When the first page became available to a worker, measured from the job row
    // rather than from this function's start, so it includes the sweep's wait for
    // the next tick. That wait is part of what the submitter experiences and would
    // otherwise be invisible - it happens before the stopwatch here does.
    let firstBatchMs: number | null = null;
    // Per-page cost, tracked as running figures rather than kept in an array. A
    // 500,000 record job is 500 pages, and the only question the numbers answer
    // is whether the walk stays linear as the cursor advances - so the spread is
    // what matters, not every sample. The mean comes from the total instead of
    // from these, because it then includes the final read that ends the walk.
    let pageMinMs = Infinity;
    let pageMaxMs = 0;

    const resumeFrom = cursor;
    if (resumeFrom) {
      const prior = await this.db.query<{ n: number }>(
        `SELECT coalesce(max(batch_no), -1)::int AS n FROM bulk_job_outbox
          WHERE job_id = $1 AND workspace_id = $2`,
        [jobId, workspaceId],
      );
      batchNo = (prior[0]?.n ?? -1) + 1;
    }

    for (;;) {
      // Started before the read, not after it. A page's cost is the read plus the
      // write plus the commit, and stopping the clock after the read would report
      // only the write - which is the cheap half, and would make the walk look
      // far more uniform than it is.
      const pageStart = startTimer();
      const page = await pageMatching(
        query,
        workspaceId,
        filter,
        job.snapshot_at,
        cursor,
        BATCH_SIZE,
      );
      if (page.length === 0) break;

      const last = page[page.length - 1]!;
      const nextCursor = { createdAt: last.created_at, id: last.id };

      await this.db.transaction(async (client) => {
        await client.query(
          `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
           VALUES ($1, $2, $3, $4::uuid[])
           ON CONFLICT (job_id, batch_no) DO NOTHING`,
          [workspaceId, jobId, batchNo, page.map((r) => r.id)],
        );
        // Same transaction as the batch. A cursor ahead of the data would skip
        // records on resume; a cursor behind it would duplicate them.
        await client.query(
          `UPDATE bulk_job
              SET snapshot_cursor = $3, snapshot_cursor_id = $4, updated_at = now()
            WHERE id = $1 AND workspace_id = $2`,
          [jobId, workspaceId, nextCursor.createdAt, nextCursor.id],
        );
      });

      const pageMs = elapsedMs(pageStart);
      if (pageMs < pageMinMs) pageMinMs = pageMs;
      if (pageMs > pageMaxMs) pageMaxMs = pageMs;

      matched += page.length;
      cursor = nextCursor;
      batchNo += 1;
      if (firstBatchMs === null) {
        // Wall clock, not the stopwatch: job.created_at was written by Postgres on
        // a different connection, so only a wall-clock delta spans the two. The
        // containers share one host clock, so the skew is nil, and the alternative
        // - a timestamp column written by this process - would be a second source
        // of truth for something the job row already records.
        firstBatchMs = Date.now() - job.created_at.getTime();
      }
      if (page.length < BATCH_SIZE) break;
    }

    // total_matched is what actually landed in batches, not a count taken before
    // the walk. A record can leave the filter while the walk is running, so a
    // pre-count would be a number that was already stale by the time it was sent.
    await this.db.transaction(async (client) => {
      await client.query(
        `UPDATE bulk_job j
            SET total_matched = COALESCE(agg.records, 0),
                snapshot_cursor = NULL,
                snapshot_cursor_id = NULL,
                status = CASE
                  WHEN COALESCE(agg.batches, 0) = 0 THEN 'completed'
                  ELSE 'pending'
                END,
                completed_at = CASE
                  WHEN COALESCE(agg.batches, 0) = 0 THEN now()
                  ELSE NULL
                END,
                updated_at = now()
           FROM (SELECT count(*)::int AS batches, coalesce(sum(cardinality(item_ids)), 0)::int AS records
                   FROM bulk_job_outbox WHERE job_id = $1 AND workspace_id = $2) agg
          WHERE j.id = $1 AND j.workspace_id = $2
            -- Preparing, and only preparing, for the same reason the read above
            -- checks it. A walk that started while this job was genuinely
            -- preparing can still be running when something else finalises it -
            -- an operator resetting the job, or a second builder that got in
            -- before the entry check existed. Without this the finalise would
            -- drag a job the workers had already moved to running back to
            -- pending, and clear a completed_at that was set. The worker would
            -- eventually settle it again, but in between a caller polling the
            -- status would be told a running job had not started.
            AND j.status = 'preparing'`,
        [jobId, workspaceId],
      );
    });

    // The duration is on the line because the log is the only place it exists.
    // Nothing stored on the job row says when the build finished: the cursor is
    // nulled, and updated_at is overwritten by the worker's counter deltas
    // within milliseconds of the first batch landing. The last batch's
    // created_at is a close proxy, but it is a proxy, and a build that walked
    // nothing records no batch at all.
    //
    // Per-page and first-batch are here for the same reason. The per-page figure
    // is what makes the walk's linearity checkable rather than assumed - at 50
    // batches it should be flat, and if it is not, the walk is not the cost. And
    // time-to-first-batch is the number a submitter actually feels: the total
    // says how long the job took to become complete, this says how long before
    // any of it started moving.
    const totalMs = elapsedMs(started);
    const perPage = batchNo > 0 ? totalMs / batchNo : 0;
    const first = firstBatchMs === null ? 'never' : ms1(firstBatchMs);
    // The spread, not just the mean. A flat walk means the cost is the pages and
    // 500,000 records is 500 of them; a spread that widens as the cursor advances
    // means something is degrading with depth, and the mean hides that completely.
    const spread =
      batchNo > 0 ? `${ms1(pageMinMs)}/${ms1(perPage)}/${ms1(pageMaxMs)} min/mean/max` : 'none';
    this.logger.log(
      `job ${jobId} snapshot built: ${matched} records in ${batchNo} batches ` +
        `in ${ms1(totalMs)} (page ${spread}, first batch at ${first}) ` +
        `id=${job.correlation_id ?? 'none'}`,
    );
  }
}
