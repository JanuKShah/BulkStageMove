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
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
}

const COLUMNS =
  'id, workspace_id, idempotency_key, filter, target_stage_id, status, total_matched, ' +
  'processed_count, failed_count, attempts, error, created_at, started_at, completed_at';

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
  }): Promise<BulkJob> {
    const rows = await this.db.query<BulkJob>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id)
       VALUES ($1, $2, $3, $4) RETURNING ${COLUMNS}`,
      [input.workspaceId, input.idempotencyKey, JSON.stringify(input.filter), input.targetStageId],
    );
    return rows[0]!;
  }

  async addToTotal(workspaceId: string, jobId: string, count: number): Promise<void> {
    await this.db.query(
      `UPDATE bulk_job
       SET total_matched = total_matched + $3, updated_at = now()
       WHERE id = $1 AND workspace_id = $2`,
      [jobId, workspaceId, count],
    );
  }

  async findById(workspaceId: string, id: string): Promise<BulkJob | null> {
    const rows = await this.db.query<BulkJob>(
      `SELECT ${COLUMNS} FROM bulk_job WHERE id = $1 AND workspace_id = $2`,
      [id, workspaceId],
    );
    return rows[0] ?? null;
  }

  /** Counts straight from the items, which is committed state rather than a counter. */
  async statusCounts(workspaceId: string, jobId: string): Promise<Record<string, number>> {
    const rows = await this.db.query<{ status: string; n: number }>(
      `SELECT status, count(*)::int AS n FROM bulk_job_item
       WHERE job_id = $1 AND workspace_id = $2 GROUP BY status`,
      [jobId, workspaceId],
    );
    return Object.fromEntries(rows.map((r) => [r.status, r.n]));
  }

  /**
   * Failed items, with the reason and the batch they belonged to.
   *
   * A batch is applied whole or not at all, so a refusal fails every record in
   * it. That makes "which records do I fix" answerable only by naming the batch
   * and the rule that blocked it, which is what this returns.
   */
  async failures(
    workspaceId: string,
    jobId: string,
    limit: number,
  ): Promise<
    {
      batch_no: number;
      opportunity_id: string;
      name: string;
      from_stage_id: string;
      error: string | null;
      attempts: number;
    }[]
  > {
    return this.db.query(
      `SELECT i.batch_no, i.opportunity_id, o.name, i.from_stage_id, i.error, i.attempts
         FROM bulk_job_item i
         JOIN opportunity o ON o.id = i.opportunity_id AND o.workspace_id = i.workspace_id
        WHERE i.job_id = $1 AND i.workspace_id = $2 AND i.status = 'failed'
        ORDER BY i.batch_no, o.name
        LIMIT $3`,
      [jobId, workspaceId, limit],
    );
  }

  /** Per-batch rollup, so a job of 50 batches can be read at a glance. */
  async batchSummary(
    workspaceId: string,
    jobId: string,
  ): Promise<
    { batch_no: number; total: number; completed: number; failed: number; pending: number }[]
  > {
    return this.db.query(
      `SELECT batch_no,
              count(*)::int AS total,
              count(*) FILTER (WHERE status = 'completed')::int AS completed,
              count(*) FILTER (WHERE status = 'failed')::int    AS failed,
              count(*) FILTER (WHERE status IN ('pending','running'))::int AS pending
         FROM bulk_job_item
        WHERE job_id = $1 AND workspace_id = $2
        GROUP BY batch_no
        ORDER BY batch_no`,
      [jobId, workspaceId],
    );
  }
}
