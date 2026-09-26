import { Injectable } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../../shared/database/database.service';
import type { ParsedFilter } from '../../shared/filter/opportunity-filter';

export interface OpportunityRef {
  id: string;
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
      params.push(after.createdAt, after.id);
      where += ` AND (created_at, id) > ($${params.length - 1}, $${params.length})`;
    }
    params.push(limit);

    return this.db.query<OpportunityRef>(
      `SELECT id, created_at FROM opportunity WHERE ${where}
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
  async insertPage(
    client: PoolClient,
    workspaceId: string,
    jobId: string,
    opportunityIds: string[],
  ): Promise<number> {
    if (opportunityIds.length === 0) return 0;
    const result = await client.query(
      `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id)
       SELECT $1, $2, o FROM unnest($3::uuid[]) AS o
       ON CONFLICT (job_id, opportunity_id) DO NOTHING`,
      [jobId, workspaceId, opportunityIds],
    );
    return result.rowCount ?? 0;
  }
}
