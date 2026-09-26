import { Injectable } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../../shared/database/database.service';
import type { ParsedFilter } from '../../shared/filter/opportunity-filter';

export interface OpportunityRef {
  id: string;
  stage_id: string;
  created_at: Date;
}

@Injectable()
export class BulkJobItemRepository {
  constructor(private readonly db: DatabaseService) {}

  /**
   * One page of matching opportunities, ascending on (created_at, id) so the
   * sweep is stable and the cursor never revisits a row. The filter is applied
   * inside the query rather than by paging the API, because a 50,000-row
   * response over HTTP is not a reasonable thing to move around.
   */
  async page(
    workspaceId: string,
    filter: ParsedFilter,
    after: { createdAt: Date; id: string } | null,
    limit: number,
  ): Promise<OpportunityRef[]> {
    const params: unknown[] = [workspaceId];
    let where = 'workspace_id = $1';

    const add = (clause: string, value: unknown): void => {
      params.push(value);
      where += ` AND ${clause.replace('$?', `$${params.length}`)}`;
    };

    if (filter.stageIds?.length) add('stage_id = ANY($?::uuid[])', filter.stageIds);
    if (filter.ownerIds?.length) add('owner_id = ANY($?::uuid[])', filter.ownerIds);
    if (filter.minValue !== undefined) add('value >= $?', filter.minValue);
    if (filter.maxValue !== undefined) add('value <= $?', filter.maxValue);
    if (filter.createdFrom) add('created_at >= $?', filter.createdFrom);
    if (filter.createdTo) add('created_at <= $?', filter.createdTo);
    if (after) {
      // The cursor's position is resolved by a subquery, never by binding
      // after.createdAt. created_at is timestamptz and carries microseconds
      // while a JS Date carries milliseconds, so binding it truncates the value:
      // every remaining row then compares greater than the truncated cursor, the
      // same page comes back for ever, and every insert conflicts. The job spins
      // and reports completed having moved a fraction of what it matched.
      params.push(after.id);
      where += ` AND (created_at, id) > (
        SELECT created_at, id FROM opportunity WHERE id = $${params.length} AND workspace_id = $1
      )`;
    }
    params.push(limit);

    return this.db.query<OpportunityRef>(
      `SELECT id, stage_id, created_at FROM opportunity WHERE ${where}
       ORDER BY created_at, id LIMIT $${params.length}`,
      params,
    );
  }

  async count(workspaceId: string, filter: ParsedFilter): Promise<number> {
    const params: unknown[] = [workspaceId];
    let where = 'workspace_id = $1';
    const add = (clause: string, value: unknown): void => {
      params.push(value);
      where += ` AND ${clause.replace('$?', `$${params.length}`)}`;
    };
    if (filter.stageIds?.length) add('stage_id = ANY($?::uuid[])', filter.stageIds);
    if (filter.ownerIds?.length) add('owner_id = ANY($?::uuid[])', filter.ownerIds);
    if (filter.minValue !== undefined) add('value >= $?', filter.minValue);
    if (filter.maxValue !== undefined) add('value <= $?', filter.maxValue);
    if (filter.createdFrom) add('created_at >= $?', filter.createdFrom);
    if (filter.createdTo) add('created_at <= $?', filter.createdTo);

    const rows = await this.db.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM opportunity WHERE ${where}`,
      params,
    );
    return rows[0]?.n ?? 0;
  }

  /**
   * Inserts a page of items. The job id and workspace are repeated on every row
   * so the composite foreign keys can enforce that each opportunity belongs to
   * the job's tenant.
   */
  /** Records each opportunity with the stage it is in right now, as the snapshot. */
  async insertPage(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
    refs: { id: string; stage_id: string }[],
  ): Promise<number> {
    if (refs.length === 0) return 0;
    const result = await client.query(
      `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id, from_stage_id, batch_no)
       SELECT $1, $2, o, s, $4 FROM unnest($3::uuid[], $5::uuid[]) AS t(o, s)
       ON CONFLICT (job_id, opportunity_id) DO NOTHING`,
      [jobId, workspaceId, refs.map((r) => r.id), batchNo, refs.map((r) => r.stage_id)],
    );
    return result.rowCount ?? 0;
  }

  /**
   * Claims a batch by moving its pending items to running and returning the
   * opportunities that were actually claimed.
   *
   * This is the whole concurrency story: the status='pending' predicate makes
   * the claim a compare-and-set, so a redelivered or duplicated message claims
   * nothing and does no work. It is why batch-level retry is safe even though
   * completion is tracked per item - a retried batch skips what already finished.
   *
   * Back on that partial index, which is why it exists.
   */
  async claimBatch(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    batchNo: number,
  ): Promise<string[]> {
    const result = await client.query<{ opportunity_id: string }>(
      `UPDATE bulk_job_item
          SET status = 'running', attempts = attempts + 1, updated_at = now()
        WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3 AND status = 'pending'
        RETURNING opportunity_id`,
      [jobId, batchNo, workspaceId],
    );
    return result.rows.map((r) => r.opportunity_id);
  }

  /** Resolves a claimed batch back to its item ids, for completion reporting. */
  async itemIdsForBatch(workspaceId: string, jobId: string, batchNo: number): Promise<string[]> {
    const rows = await this.db.query<{ id: string }>(
      'SELECT id FROM bulk_job_item WHERE job_id = $1 AND batch_no = $2 AND workspace_id = $3',
      [jobId, batchNo, workspaceId],
    );
    return rows.map((r) => r.id);
  }
}
