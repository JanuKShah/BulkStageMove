import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';

export interface BulkJob {
  id: string;
  workspace_id: string;
  idempotency_key: string;
  filter: unknown;
  target_stage_id: string;
  status: string;
  total_matched: number;
  processed_count: number;
  failed_count: number;
  attempts: number;
  error: string | null;
  snapshot_at: Date;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
}

const COLUMNS =
  'id, workspace_id, idempotency_key, filter, target_stage_id, status, total_matched, ' +
  'processed_count, failed_count, attempts, error, snapshot_at, created_at, started_at, ' +
  'completed_at';

@Injectable()
export class BulkJobRepository {
  constructor(private readonly db: DatabaseService) {}

  /**
   * Lookup is by the caller's idempotency key, not by the filter. Several
   * unfinished jobs may exist in one workspace; the key is what identifies a
   * retry of a specific submission.
   */
  async findByIdempotencyKey(workspaceId: string, key: string): Promise<BulkJob | null> {
    const rows = await this.db.query<BulkJob>(
      `SELECT ${COLUMNS} FROM bulk_job WHERE workspace_id = $1 AND idempotency_key = $2`,
      [workspaceId, key],
    );
    return rows[0] ?? null;
  }

  async create(input: {
    workspaceId: string;
    idempotencyKey: string;
    filter: unknown;
    targetStageId: string;
    /**
     * The submitting request's correlation id, stored so the build can be traced
     * back to it. The build runs minutes later from a timer, outside the async
     * chain that held the id, so this is the only way the two are ever connected.
     *
     * Not validated here because it cannot be unvalidated by the time it arrives:
     * the edge and the middleware both accept it only against
     * ^[A-Za-z0-9._:-]{1,64}$, and anything else was replaced with a fresh uuid
     * before it was ever stored. Re-checking would guard against nothing.
     */
    correlationId?: string;
  }): Promise<BulkJob> {
    // status is left to the column default, which is 'preparing' - the state a
    // job is actually in when it is created, since its batches do not exist yet
    // and SnapshotBuilder writes them. Saying it here as well would be a second
    // place to change it when the lifecycle does.
    const rows = await this.db.query<BulkJob>(
      `INSERT INTO bulk_job
         (workspace_id, idempotency_key, filter, target_stage_id, snapshot_at, correlation_id)
       VALUES ($1, $2, $3, $4, now(), $5) RETURNING ${COLUMNS}`,
      [
        input.workspaceId,
        input.idempotencyKey,
        JSON.stringify(input.filter),
        input.targetStageId,
        input.correlationId ?? null,
      ],
    );
    return rows[0]!;
  }

  async findById(workspaceId: string, id: string): Promise<BulkJob | null> {
    const rows = await this.db.query<BulkJob>(
      `SELECT ${COLUMNS} FROM bulk_job WHERE id = $1 AND workspace_id = $2`,
      [id, workspaceId],
    );
    return rows[0] ?? null;
  }

  /**
   * Batch counts by status, aggregated over the job's 50 batch rows.
   *
   * The unit is batches, not records, and that is the honest one now that there
   * is no per-record state. Record counts live on the job row as
   * processed_count and failed_count, and per batch as completed_count and
   * failed_count on the batch row itself.
   */
  async statusCounts(workspaceId: string, jobId: string): Promise<Record<string, number>> {
    const rows = await this.db.query<{ status: string; n: number }>(
      `SELECT status, count(*)::int AS n FROM bulk_job_outbox
       WHERE job_id = $1 AND workspace_id = $2 GROUP BY status`,
      [jobId, workspaceId],
    );
    return Object.fromEntries(rows.map((r) => [r.status, r.n]));
  }

  /**
   * Batches that exhausted their attempt budget, which is what it means for a
   * message to reach the dead letter queue.
   *
   * The distinction matters because "failed" on a batch row is overloaded and the
   * two cases call for different responses:
   *
   *   status = 'completed' with failed_count > 0
   *     The batch did its job. Some records were unmovable, which is a business
   *     answer, reported per record by `failures`.
   *
   *   status = 'failed'
   *     The batch never completed. It was attempted RABBITMQ_MAX_ATTEMPTS times
   *     and the worker gave up, and the message went to the DLQ. That is an
   *     infrastructure or contention problem, and those records are in neither
   *     per-record failures nor the job's counters - they are simply not moved.
   *
   * Nothing consumes the DLQ, deliberately: a batch that has exhausted its budget
   * should not be resurrected automatically, and the queue's own TTL and length
   * cap are how it is eventually discarded. So the batch row is the only durable
   * record that it happened, and this is the only way a caller learns of it
   * without paging all fifty batches.
   */
  async deadLettered(
    workspaceId: string,
    jobId: string,
  ): Promise<{
    batches: number;
    records: number;
    reasons: { reason: string; batches: number }[];
  }> {
    const summary = await this.db.query<{ batches: number; records: number }>(
      `SELECT count(*)::int AS batches, coalesce(sum(failed_count), 0)::int AS records
         FROM bulk_job_outbox
        WHERE job_id = $1 AND workspace_id = $2 AND status = 'failed'`,
      [jobId, workspaceId],
    );

    const reasons = await this.db.query<{ reason: string; batches: number }>(
      `SELECT coalesce(error, 'no reason recorded') AS reason, count(*)::int AS batches
         FROM bulk_job_outbox
        WHERE job_id = $1 AND workspace_id = $2 AND status = 'failed'
        GROUP BY error
        ORDER BY batches DESC, reason
        LIMIT 10`,
      [jobId, workspaceId],
    );

    return {
      batches: summary[0]?.batches ?? 0,
      records: summary[0]?.records ?? 0,
      reasons: reasons.map((r) => ({ reason: r.reason, batches: r.batches })),
    };
  }

  /**
   * The records that could not be moved, and why.
   *
   * A table of exceptions, not of the population: bulk_job_failure is empty for
   * a clean job, so this is a small read where it used to be a filtered scan of
   * 50,000 rows.
   *
   * The join to opportunity is a LEFT JOIN and the name is nullable, because a
   * record can fail precisely by no longer existing. An inner join would drop
   * exactly the failure most worth reporting.
   */
  async failures(
    workspaceId: string,
    jobId: string,
    limit: number,
  ): Promise<
    {
      batch_no: number;
      opportunity_id: string;
      name: string | null;
      from_stage_id: string | null;
      error: string;
      attempts: number;
    }[]
  > {
    return this.db.query(
      `SELECT f.batch_no, f.opportunity_id, o.name, f.from_stage_id, f.error, f.attempts
         FROM bulk_job_failure f
         LEFT JOIN opportunity o ON o.id = f.opportunity_id AND o.workspace_id = f.workspace_id
        WHERE f.job_id = $1 AND f.workspace_id = $2
        ORDER BY f.batch_no, o.name NULLS LAST, f.opportunity_id
        LIMIT $3`,
      [jobId, workspaceId, limit],
    );
  }

  /**
   * Per-batch rollup, read straight off the batch rows.
   *
   * pending is derived as item_count - completed - failed rather than counted,
   * so it is the work still outstanding. It can read low next to what the job
   * row reports if records stopped matching the filter, which is the documented
   * consequence of evaluating the predicate live.
   */
  async batchSummary(
    workspaceId: string,
    jobId: string,
  ): Promise<
    {
      batch_no: number;
      total: number;
      completed: number;
      failed: number;
      pending: number;
      status: string;
      attempts: number;
      error: string | null;
    }[]
  > {
    return this.db.query(
      `SELECT batch_no,
              item_count AS total,
              completed_count AS completed,
              failed_count AS failed,
              GREATEST(item_count - completed_count - failed_count, 0)::int AS pending,
              status,
              attempts,
              error
         FROM bulk_job_outbox
        WHERE job_id = $1 AND workspace_id = $2
        ORDER BY batch_no`,
      [jobId, workspaceId],
    );
  }
}
