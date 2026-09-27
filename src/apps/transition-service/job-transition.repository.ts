import { BadRequestException, Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { decodeCursor, encodeCursor } from '../../shared/filter/keyset-cursor';

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
    cursor: string | null,
  ): Promise<{ items: JobTransition[]; hasMore: boolean; nextCursor: string | null }> {
    const params: unknown[] = [workspaceId, jobId];
    let where = 't.workspace_id = $1 AND t.job_id = $2';

    if (cursor) {
      // The position is bound, not re-read. The subquery this replaced returned
      // NULL once the cursor row was gone - and a job's transitions go with the job
      // on a cascade delete - which made the comparison UNKNOWN and dropped every
      // remaining row with no error anywhere.
      //
      // created_at is carried as the database's own timestamptz text and bound back
      // as a timestamptz. A JS Date holds milliseconds and timestamptz holds
      // microseconds, so binding one truncates the cursor to below its own row and
      // that row reappears on the next page.
      const at = decodeCursor(cursor);
      if (!at) throw new BadRequestException('cursor is not a valid pagination cursor');
      params.push(at.createdAt, at.id);
      where += ` AND (t.created_at, t.id) > ($${params.length - 1}::timestamptz, $${params.length}::uuid)`;
    }
    params.push(limit + 1);

    const rows = await this.db.query<JobTransition & { cursor_at: string }>(
      `SELECT t.id, t.opportunity_id, t.from_stage_id AS from_stage, t.to_stage_id AS to_stage,
              fs.name AS from_stage_name, ts.name AS to_stage_name,
              fs.outcome AS from_outcome, ts.outcome AS to_outcome, t.created_at,
              t.created_at::text AS cursor_at
       FROM opportunity_transition t
       LEFT JOIN stage fs ON fs.id = t.from_stage_id
       JOIN stage ts ON ts.id = t.to_stage_id
       WHERE ${where}
       ORDER BY t.created_at, t.id
       LIMIT $${params.length}`,
      params,
    );
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return {
      items: page,
      hasMore: rows.length > limit,
      // Built here so cursor_at stays an internal detail of the position.
      nextCursor:
        rows.length > limit && last
          ? encodeCursor({ createdAt: last.cursor_at, id: last.id })
          : null,
    };
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
