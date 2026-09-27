import { BadRequestException, Injectable } from '@nestjs/common';
import type { PoolClient } from 'pg';
import { DatabaseService } from '../../shared/database/database.service';
import { decodeCursor, encodeCursor } from '../../shared/filter/keyset-cursor';
import type { Outcome } from '../../shared/filter/opportunity-filter';

export interface Opportunity {
  id: string;
  workspace_id: string;
  stage_id: string;
  name: string;
  value: string;
  owner_id: string | null;
  created_at: Date;
  updated_at: Date;
  /**
   * The status configured on the stage this opportunity sits in, joined in
   * rather than stored. A deal's status is the outcome of its stage, so the two
   * cannot drift - which is the reason there is no status column to keep in sync
   * when a move happens.
   */
  status: string;
}

export interface Transition {
  id: string;
  opportunity_id: string;
  from_stage_id: string | null;
  to_stage_id: string;
  created_at: Date;
}

export interface ListFilter {
  stageIds?: string[] | undefined;
  ownerIds?: string[] | undefined;
  outcome?: Outcome | undefined;
  minValue?: number | undefined;
  maxValue?: number | undefined;
  createdFrom?: Date | undefined;
  createdTo?: Date | undefined;
  limit: number;
  cursor?: string | undefined;
}

/**
 * The opportunity's own columns. Used by INSERT/UPDATE ... RETURNING, which
 * cannot reference another table, so those two cannot return the joined status
 * and the service re-reads when it needs it.
 */
const COLUMNS = 'id, workspace_id, stage_id, name, value, owner_id, created_at, updated_at';

/**
 * The same columns plus the stage's name and outcome, for the read paths.
 *
 * status is the outcome of the stage the opportunity is in, selected through the
 * join rather than looked up per record. That is what makes it impossible for a
 * move to leave a stale status behind: there is no separate stored value to keep
 * in sync, so a caller cannot observe a status that disagrees with the stage.
 */
const READ_COLUMNS = `o.id, o.workspace_id, o.stage_id, o.name, o.value, o.owner_id,
       o.created_at, o.updated_at, s.name AS stage_name, s.outcome AS status`;
const FROM_JOINED = 'FROM opportunity o JOIN stage s ON s.id = o.stage_id';

@Injectable()
export class OpportunityRepository {
  constructor(private readonly db: DatabaseService) {}

  async findById(workspaceId: string, id: string): Promise<Opportunity | null> {
    const rows = await this.db.query<Opportunity>(
      `SELECT ${READ_COLUMNS} ${FROM_JOINED} WHERE o.id = $1 AND o.workspace_id = $2`,
      [id, workspaceId],
    );
    return rows[0] ?? null;
  }

  async list(
    workspaceId: string,
    filter: ListFilter,
  ): Promise<{ items: Opportunity[]; hasMore: boolean; nextCursor: string | null }> {
    const params: unknown[] = [workspaceId];
    let where = 'o.workspace_id = $1';

    // outcome is not stored on the row - it lives on the stage. The service
    // resolves it to stage ids first, so this stays a plain equality on
    // stage_id and keeps the query index-friendly.
    if (filter.stageIds?.length) {
      params.push(filter.stageIds);
      where += ` AND o.stage_id = ANY($${params.length}::uuid[])`;
    }
    if (filter.ownerIds?.length) {
      params.push(filter.ownerIds);
      where += ` AND o.owner_id = ANY($${params.length}::uuid[])`;
    }
    if (filter.minValue !== undefined) {
      params.push(filter.minValue);
      where += ` AND o.value >= $${params.length}`;
    }
    if (filter.maxValue !== undefined) {
      params.push(filter.maxValue);
      where += ` AND o.value <= $${params.length}`;
    }
    if (filter.createdFrom) {
      params.push(filter.createdFrom);
      where += ` AND o.created_at >= $${params.length}`;
    }
    if (filter.createdTo) {
      params.push(filter.createdTo);
      where += ` AND o.created_at <= $${params.length}`;
    }
    if (filter.cursor) {
      // Keyset pagination on (created_at, id) stays stable while rows are being
      // inserted, unlike OFFSET which can skip or repeat under concurrent writes.
      //
      // The position travels in the cursor rather than being re-read from the row
      // it points at. The subquery this replaced returned NULL if that row had
      // been deleted - and a deleted cursor row is ordinary here, since deleting a
      // workspace cascades to its opportunities - which made the comparison
      // UNKNOWN and dropped every remaining row. A caller paging a large set got
      // a short list and no error.
      //
      // The timestamp is text in the database's own timestamptz form, bound back
      // as a timestamptz. A JS Date holds milliseconds where timestamptz holds
      // microseconds, so binding one would truncate the cursor below its own row
      // and the same page would return for ever.
      const at = decodeCursor(filter.cursor);
      if (!at) throw new BadRequestException('cursor is not a valid pagination cursor');
      params.push(at.createdAt, at.id);
      where += ` AND (o.created_at, o.id) < ($${params.length - 1}::timestamptz, $${params.length}::uuid)`;
    }
    params.push(filter.limit + 1);

    const rows = await this.db.query<Opportunity & { cursor_at: string }>(
      // cursor_at is the lossless timestamp for the next page. The response's own
      // created_at stays a timestamptz, because that is what a client reads.
      `SELECT ${READ_COLUMNS}, o.created_at::text AS cursor_at ${FROM_JOINED} WHERE ${where}
       ORDER BY o.created_at DESC, o.id DESC LIMIT $${params.length}`,
      params,
    );
    // The cursor is built here rather than in the service, so cursor_at never
    // reaches the client: it is an implementation detail of the position, and the
    // response should carry the opaque token and nothing else.
    const page = rows.slice(0, filter.limit);
    const last = page.at(-1);
    return {
      items: page,
      hasMore: rows.length > filter.limit,
      nextCursor:
        rows.length > filter.limit && last
          ? encodeCursor({ createdAt: last.cursor_at, id: last.id })
          : null,
    };
  }

  async listTransitions(workspaceId: string, opportunityId: string): Promise<Transition[]> {
    return this.db.query<Transition>(
      `SELECT id, opportunity_id, from_stage_id, to_stage_id, created_at
       FROM opportunity_transition
       WHERE opportunity_id = $1 AND workspace_id = $2
       ORDER BY created_at`,
      [opportunityId, workspaceId],
    );
  }

  insert(
    client: PoolClient,
    input: {
      id: string;
      workspaceId: string;
      stageId: string;
      name: string;
      value: number;
      ownerId: string | null;
    },
  ): Promise<Opportunity> {
    return client
      .query<Opportunity>(
        `INSERT INTO opportunity (id, workspace_id, stage_id, name, value, owner_id)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING ${COLUMNS}`,
        [input.id, input.workspaceId, input.stageId, input.name, input.value, input.ownerId],
      )
      .then((r) => r.rows[0]!);
  }

  updateStage(
    client: PoolClient,
    workspaceId: string,
    id: string,
    stageId: string,
  ): Promise<Opportunity | null> {
    return client
      .query<Opportunity>(
        `UPDATE opportunity SET stage_id = $1, updated_at = now()
         WHERE id = $2 AND workspace_id = $3 RETURNING ${COLUMNS}`,
        [stageId, id, workspaceId],
      )
      .then((r) => r.rows[0] ?? null);
  }

  insertTransition(
    client: PoolClient,
    input: { workspaceId: string; opportunityId: string; from: string | null; to: string },
  ): Promise<void> {
    return client
      .query(
        `INSERT INTO opportunity_transition (workspace_id, opportunity_id, from_stage_id, to_stage_id)
         VALUES ($1, $2, $3, $4)`,
        [input.workspaceId, input.opportunityId, input.from, input.to],
      )
      .then(() => undefined);
  }

  /** One query for the whole batch, rather than one per record. */
  async findByIds(workspaceId: string, ids: string[]): Promise<{ id: string; stage_id: string }[]> {
    return this.db.query<{ id: string; stage_id: string }>(
      'SELECT id, stage_id FROM opportunity WHERE workspace_id = $1 AND id = ANY($2::uuid[])',
      [workspaceId, ids],
    );
  }

  updateStages(
    client: PoolClient,
    workspaceId: string,
    ids: string[],
    stageId: string,
  ): Promise<number> {
    return client
      .query(
        `UPDATE opportunity SET stage_id = $1, updated_at = now()
          WHERE workspace_id = $2 AND id = ANY($3::uuid[])`,
        [stageId, workspaceId, ids],
      )
      .then((r) => r.rowCount ?? 0);
  }

  /**
   * One insert for the whole batch. job_id is passed so the transitions a bulk
   * job caused are attributable to it; manual moves leave it null.
   */
  insertTransitions(
    client: PoolClient,
    workspaceId: string,
    rows: { opportunityId: string; from: string }[],
    toStageId: string,
    jobId: string | null,
  ): Promise<number> {
    if (rows.length === 0) return Promise.resolve(0);
    return client
      .query(
        `INSERT INTO opportunity_transition
           (workspace_id, opportunity_id, from_stage_id, to_stage_id, job_id)
         SELECT $1, o, f, $2, $3::uuid
           FROM unnest($4::uuid[], $5::uuid[]) AS t(o, f)`,
        [workspaceId, toStageId, jobId, rows.map((r) => r.opportunityId), rows.map((r) => r.from)],
      )
      .then((r) => r.rowCount ?? 0);
  }
}
