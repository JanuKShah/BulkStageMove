import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';

export interface JobTransition {
  id: string;
  opportunity_id: string;
  from_stage: string | null;
  to_stage: string;
  from_stage_name: string | null;
  to_stage_name: string;
  from_outcome: string | null;
  to_outcome: string;
  created_at: Date;
}

@Injectable()
export class JobTransitionRepository {
  constructor(private readonly db: DatabaseService) {}

  /**
   * Transitions attributable to one bulk job. Stage names and outcomes are
   * joined in because the point of this endpoint is to show what the job did,
   * and a wall of uuids answers nothing.
   *
   * Keyset paginated on (created_at, id): a 50,000-row job produces 50,000
   * transitions, and OFFSET would skip or repeat rows as the job writes more.
   *
   * The cursor's position is resolved by a subquery rather than by reading its
   * timestamp into the client. timestamptz carries microseconds and a JS Date
   * only milliseconds, so round-tripping the value would truncate it and the
   * cursor row would satisfy the comparison and appear on two pages.
   */
  async list(
    workspaceId: string,
    jobId: string,
    limit: number,
    cursorId: string | null,
  ): Promise<{ items: JobTransition[]; hasMore: boolean }> {
    const params: unknown[] = [workspaceId, jobId];
    let where = 't.workspace_id = $1 AND t.job_id = $2';

    if (cursorId) {
      params.push(cursorId);
      where += ` AND (t.created_at, t.id) > (
        SELECT created_at, id FROM opportunity_transition
        WHERE id = $3 AND workspace_id = $1 AND job_id = $2
      )`;
    }
    params.push(limit + 1);

    const rows = await this.db.query<JobTransition>(
      `SELECT t.id, t.opportunity_id, t.from_stage_id AS from_stage, t.to_stage_id AS to_stage,
              fs.name AS from_stage_name, ts.name AS to_stage_name,
              fs.outcome AS from_outcome, ts.outcome AS to_outcome, t.created_at
       FROM opportunity_transition t
       LEFT JOIN stage fs ON fs.id = t.from_stage_id
       JOIN stage ts ON ts.id = t.to_stage_id
       WHERE ${where}
       ORDER BY t.created_at, t.id
       LIMIT $${params.length}`,
      params,
    );
    return { items: rows.slice(0, limit), hasMore: rows.length > limit };
  }

  /** Rejects a cursor that is not part of this job, instead of silently returning an empty page. */
  async cursorBelongsToJob(workspaceId: string, jobId: string, cursorId: string): Promise<boolean> {
    const rows = await this.db.query<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM opportunity_transition
         WHERE id = $1 AND workspace_id = $2 AND job_id = $3
       ) AS exists`,
      [cursorId, workspaceId, jobId],
    );
    return rows[0]?.exists ?? false;
  }
}
