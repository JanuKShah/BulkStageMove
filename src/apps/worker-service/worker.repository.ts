import { Inject, Injectable } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../../shared/database/database.service';
import { RABBIT_CONFIG, type RabbitConfig } from '../../shared/rabbit/rabbit.config';

export interface ClaimedItem {
  id: string;
  opportunity_id: string;
  from_stage_id: string;
  attempts: number;
}

/** What the worker tells the broker about a batch. */
export type BatchDisposition = 'applied' | 'skipped' | 'dead' | 'retry';

export interface BatchResult {
  disposition: BatchDisposition;
  moved: number;
  attempts: number;
  reason?: string;
}

/**
 * All SQL the batch worker runs.
 *
 * The compare-and-swap lives in the UPDATE rather than in a prior SELECT. Reading
 * the current stages, deciding in application code and then writing would leave a
 * window where a user moves a record between the read and the write, and the
 * write would silently clobber it. Joining bulk_job_item into the UPDATE makes
 * Postgres itself refuse unless every record is still where the job found it,
 * and rowCount then says whether it did.
 */
@Injectable()
export class WorkerRepository {
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
   * it and the batch would retry until the broker gave up rather than reaching
   * the dead letter queue.
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
    const key = `${jobId}:${batchNo}`;
    const client = await this.db.connect();
    try {
      const lock = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
        [key],
      );
      if (!lock.rows[0]?.locked) {
        return { disposition: 'skipped', moved: 0, attempts: 0 };
      }
      try {
        return await this.runOn(client, workspaceId, jobId, batchNo);
      } finally {
        await client.query('SELECT pg_advisory_unlock(hashtext($1))', [key]);
      }
    } finally {
      client.release();
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
    // The predicate takes 'running' as well as 'pending'. Setting running is
    // committed here, before the work, so a rejected batch would otherwise leave
    // its items running forever and the redelivered message - which looks for
    // pending - would skip them, stranding the job. Re-claiming is safe only
    // because the advisory lock means one worker is ever in here for a batch.
    // A batch that already committed has its items completed, so it matches
    // neither and is skipped, which is what makes redelivery harmless.
    await client.query('BEGIN');
    const claimed = await client.query<ClaimedItem>(
      `UPDATE bulk_job_item
          SET status = 'running', attempts = attempts + 1, updated_at = now()
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3
          AND status IN ('pending', 'running')
        RETURNING id, opportunity_id, from_stage_id, attempts`,
      [jobId, batchNo, workspaceId],
    );
    await client.query('COMMIT');

    if (claimed.rows.length === 0) return { disposition: 'skipped', moved: 0, attempts: 0 };
    const attempts = claimed.rows[0]!.attempts;

    if (attempts > this.config.maxAttempts) {
      const failed = await this.failBatch(
        client,
        workspaceId,
        jobId,
        batchNo,
        'attempts exhausted',
      );
      return { disposition: 'dead', moved: 0, attempts, reason: `${failed} item(s) failed` };
    }

    // --- the work, one transaction: all of it or none of it
    await client.query('BEGIN');
    try {
      const moved = await this.applyOn(client, workspaceId, jobId, batchNo);
      await client.query('COMMIT');
      return { disposition: 'applied', moved, attempts };
    } catch (error) {
      await client.query('ROLLBACK');
      const reason = error instanceof Error ? error.message : 'unexpected worker failure';
      // The attempt that spends the budget is the one that records the failure.
      // Reporting 'dead' without writing it dead-letters the message and leaves
      // the items running and the job pending, with nothing left to revisit it.
      if (attempts >= this.config.maxAttempts) {
        const failed = await this.failBatch(client, workspaceId, jobId, batchNo, reason);
        return {
          disposition: 'dead',
          moved: 0,
          attempts,
          reason: `${failed} item(s) failed: ${reason}`,
        };
      }
      return { disposition: 'retry', moved: 0, attempts, reason };
    }
  }

  /**
   * Applies the batch, or throws so the transaction rolls back. There is no
   * partial outcome: either every record is still in the stage the job found it
   * and the move is permitted, and all of them land, or nothing does.
   */
  private async applyOn(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
  ): Promise<number> {
    const items = await client.query<{ opportunity_id: string; from_stage_id: string }>(
      `SELECT opportunity_id, from_stage_id FROM bulk_job_item
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3 AND status = 'running'`,
      [jobId, batchNo, workspaceId],
    );
    const claimed = items.rows;
    if (claimed.length === 0) return 0;

    // The target comes from the job row, not the message, so a hand-crafted
    // message cannot redirect a batch.
    const target = await client.query<{ target_stage_id: string }>(
      'SELECT target_stage_id FROM bulk_job WHERE id = $1 AND workspace_id = $2',
      [jobId, workspaceId],
    );
    const targetId = target.rows[0]?.target_stage_id;
    if (!targetId) throw new BatchRejected('job row is gone, so the target stage is unknown');

    // One query for the rules of every stage this batch starts from, not 1000.
    const rules = await client.query<{ from_stage_id: string; to_stage_id: string }>(
      `SELECT from_stage_id, to_stage_id FROM stage_transition_rule
        WHERE workspace_id = $1 AND from_stage_id = ANY($2::uuid[])`,
      [workspaceId, [...new Set(claimed.map((c) => c.from_stage_id))]],
    );
    const permitted = new Set(rules.rows.map((r) => `${r.from_stage_id}->${r.to_stage_id}`));

    // Refuse the whole batch rather than applying the part that would work, so
    // the counts on bulk_job describe a batch and not a mixture of two.
    const unmovable = claimed.filter((c) => !permitted.has(`${c.from_stage_id}->${targetId}`));
    if (unmovable.length > 0) {
      throw new BatchRejected(
        `${unmovable.length} of ${claimed.length} cannot move to the target stage`,
      );
    }

    const moved = await client.query<{ id: string }>(
      `UPDATE opportunity o
          SET stage_id = j.target_stage_id, updated_at = now()
         FROM bulk_job_item i, bulk_job j
        WHERE i.job_id = $1 AND i.batch_no = $2 AND i.workspace_id = $3
          AND i.status = 'running'
          AND o.id = i.opportunity_id AND o.workspace_id = i.workspace_id
          AND o.stage_id = i.from_stage_id
          AND j.id = i.job_id AND j.workspace_id = i.workspace_id
      RETURNING o.id`,
      [jobId, batchNo, workspaceId],
    );

    if (moved.rows.length !== claimed.length) {
      throw new BatchRejected(
        `${claimed.length - moved.rows.length} record(s) changed stage since submission`,
      );
    }

    await client.query(
      `INSERT INTO opportunity_transition
         (workspace_id, opportunity_id, from_stage_id, to_stage_id, job_id)
       SELECT $3, i.opportunity_id, i.from_stage_id, $4, $1
         FROM bulk_job_item i
        WHERE i.job_id = $1 AND i.batch_no = $2 AND i.workspace_id = $3 AND i.status = 'running'`,
      [jobId, batchNo, workspaceId, targetId],
    );

    await client.query(
      `UPDATE bulk_job_item
          SET status = 'completed', completed_at = now(), updated_at = now()
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3 AND status = 'running'`,
      [jobId, batchNo, workspaceId],
    );

    await client.query(
      `UPDATE bulk_job
          SET processed_count = processed_count + $2,
              status = CASE WHEN status = 'pending' THEN 'running' ELSE status END,
              started_at = COALESCE(started_at, now()),
              updated_at = now()
        WHERE id = $1 AND workspace_id = $3`,
      [jobId, moved.rows.length, workspaceId],
    );

    await this.settleJob(client, workspaceId, jobId);
    return moved.rows.length;
  }

  /**
   * Marks a batch permanently failed. Only reached once the attempt limit is
   * spent, so it is the one place items leave as 'failed' rather than pending.
   */
  private async failBatch(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
    reason: string,
  ): Promise<number> {
    await client.query('BEGIN');
    try {
      const failed = await client.query(
        `UPDATE bulk_job_item
            SET status = 'failed', error = $4, completed_at = now(), updated_at = now()
          WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3
            AND status IN ('pending','running')`,
        [jobId, batchNo, workspaceId, reason.slice(0, 500)],
      );
      await client.query(
        `UPDATE bulk_job
            SET failed_count = failed_count + $2, error = $4, updated_at = now()
          WHERE id = $1 AND workspace_id = $3`,
        [jobId, failed.rowCount ?? 0, workspaceId, reason.slice(0, 500)],
      );
      await this.settleJob(client, workspaceId, jobId);
      await client.query('COMMIT');
      return failed.rowCount ?? 0;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }

  /**
   * Moves the job to a terminal state once nothing is left to do. The NOT EXISTS
   * is evaluated under the same row lock as the update, so two workers finishing
   * the last two batches cannot both conclude the job is finished and both write
   * a terminal state.
   */
  private async settleJob(client: PoolClient, workspaceId: string, jobId: string): Promise<void> {
    await client.query(
      `UPDATE bulk_job j
          SET status = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_item i
                              WHERE i.job_id = j.id AND i.status IN ('pending','running'))
                  THEN 'running'
                WHEN j.failed_count > 0 THEN 'failed'
                ELSE 'completed'
              END,
              completed_at = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_item i
                              WHERE i.job_id = j.id AND i.status IN ('pending','running'))
                  THEN NULL
                ELSE now()
              END,
              updated_at = now()
        WHERE j.id = $1 AND j.workspace_id = $2
          AND j.status NOT IN ('completed','failed')`,
      [jobId, workspaceId],
    );
  }
}

/** A batch that cannot be applied as it stands. Retried, then dead-lettered. */
export class BatchRejected extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BatchRejected';
  }
}
