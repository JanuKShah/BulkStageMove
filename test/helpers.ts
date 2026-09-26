import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';

export const DB_URL =
  process.env.DATABASE_URL ?? 'postgres://app:app_dev_password@localhost:5432/bulk_stage_move';

export const BASE = {
  workspace: 'http://localhost:3001',
  user: 'http://localhost:3002',
  stage: 'http://localhost:3003',
  opportunity: 'http://localhost:3004',
  transition: 'http://localhost:3005',
} as const;

export const pool = new Pool({ connectionString: DB_URL, max: 4 });

export function headers(workspaceId: string): Record<string, string> {
  return { 'content-type': 'application/json', 'x-workspace-id': workspaceId };
}

export interface ApiResponse<T> {
  status: number;
  body: T;
}

export async function api<T = unknown>(
  base: string,
  path = '',
  init: RequestInit & { workspaceId?: string } = {},
): Promise<ApiResponse<T>> {
  const { workspaceId, ...rest } = init;
  const response = await fetch(`${base}${path}`, {
    ...rest,
    headers: {
      'content-type': 'application/json',
      ...(workspaceId ? { 'x-workspace-id': workspaceId } : {}),
      ...(rest.headers ?? {}),
    },
  });
  const text = await response.text();
  return {
    status: response.status,
    body: (text === '' ? null : JSON.parse(text)) as T,
  };
}

export interface TestWorkspace {
  workspaceId: string;
  /** New Lead, Contacted, Closed Won - in pipeline order. */
  stages: { newLead: string; contacted: string; closedWon: string };
}

/**
 * Provisions an isolated workspace for a test: created through the real
 * workspace-service, then given stages and transition rules directly. Tests
 * therefore never mutate the seeded benchmark data.
 */
export async function provisionWorkspace(label: string): Promise<TestWorkspace> {
  const created = await api<{ id: string }>(BASE.workspace, '/workspaces', {
    method: 'POST',
    body: JSON.stringify({ name: `test-${label}-${process.pid}-${counter()}` }),
  });
  if (created.status !== 201 && created.status !== 200) {
    throw new Error(`could not create test workspace: ${created.status}`);
  }
  const workspaceId = created.body.id;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ids: string[] = [];
    for (const name of ['New Lead', 'Contacted', 'Closed Won']) {
      const rows = await client.query<{ id: string }>(
        'INSERT INTO stage (workspace_id, name, outcome) VALUES ($1, $2, $3) RETURNING id',
        [workspaceId, name, name === 'Closed Won' ? 'won' : 'open'],
      );
      ids.push(rows.rows[0]!.id);
    }
    const [newLead, contacted, closedWon] = ids as [string, string, string];

    // Forward moves only, so any backward move is genuinely not permitted.
    const rules: [string, string][] = [
      [newLead, contacted],
      [contacted, closedWon],
    ];
    for (const [from, to] of rules) {
      await client.query(
        'INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id) VALUES ($1, $2, $3)',
        [workspaceId, from, to],
      );
    }
    await client.query('COMMIT');
    return { workspaceId, stages: { newLead, contacted, closedWon } };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function destroyWorkspace(workspaceId: string): Promise<void> {
  await pool.query('DELETE FROM workspace WHERE id = $1', [workspaceId]);
}

/**
 * Builds a job and one batch of items without publishing anything.
 *
 * The worker tests need a batch to exist without the running worker container
 * picking it up, so this writes the job, the items and the outbox row directly
 * and the tests call processBatch themselves. It deliberately mirrors what
 * TransitionService.submit does - same columns, same from_stage snapshot - so a
 * change to the submit path shows up here as a difference rather than as a
 * silently stale fixture.
 */
export async function createJobWithItems(
  workspaceId: string,
  targetStageId: string,
  opportunityIds: string[],
  options: { enqueue?: boolean } = {},
): Promise<{ jobId: string; batchNo: number; itemIds: string[] }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const job = await client.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id)
       VALUES ($1, $2, '{}'::jsonb, $3) RETURNING id`,
      [workspaceId, `test-${randomUUID()}`, targetStageId],
    );
    const jobId = job.rows[0]!.id;

    // from_stage_id snapshots where each record is now, exactly as submit does.
    const items = await client.query<{ id: string }>(
      `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id, from_stage_id, batch_no)
       SELECT $1, $2, o.id, o.stage_id, 0 FROM opportunity o
        WHERE o.id = ANY($3::uuid[]) AND o.workspace_id = $2
       RETURNING id`,
      [jobId, workspaceId, opportunityIds],
    );
    const itemIds = items.rows.map((r) => r.id);

    await client.query('UPDATE bulk_job SET total_matched = $2 WHERE id = $1', [
      jobId,
      itemIds.length,
    ]);

    if (options.enqueue !== false) {
      await client.query(
        `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_count)
         VALUES ($1, $2, 0, $3)`,
        [workspaceId, jobId, itemIds.length],
      );
    }
    await client.query('COMMIT');
    return { jobId, batchNo: 0, itemIds };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/** Creates opportunities straight in one stage, for worker fixtures. */
export async function seedOpportunitiesInStage(
  workspaceId: string,
  stageId: string,
  count: number,
): Promise<string[]> {
  if (count === 0) return [];
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO opportunity (workspace_id, stage_id, name, value)
     SELECT $1, $2, 'w-' || (row_number() over () || $3), 100
       FROM generate_series(1, $4)
     RETURNING id`,
    [workspaceId, stageId, randomUUID(), count],
  );
  return rows.map((r) => r.id);
}

export async function createOpportunity(
  workspaceId: string,
  stageId: string,
  name: string,
  extra: Record<string, unknown> = {},
): Promise<{ id: string; stage_id: string }> {
  const response = await api<{ id: string; stage_id: string }>(BASE.opportunity, '/opportunities', {
    method: 'POST',
    workspaceId,
    body: JSON.stringify({ stageId, name, ...extra }),
  });
  if (response.status >= 400) {
    throw new Error(
      `createOpportunity failed: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }
  return response.body;
}

/**
 * Creates opportunities directly, because a test workspace provisioned by
 * provisionWorkspace() has stages but no rows, so a bulk move over it would
 * match nothing.
 */
export async function seedOpportunities(
  workspaceId: string,
  stageId: string,
  count: number,
  ownerId?: string,
): Promise<void> {
  if (count === 0) return;
  const client = await pool.connect();
  try {
    for (let offset = 0; offset < count; offset += 1000) {
      const size = Math.min(1000, count - offset);
      await client.query(
        `INSERT INTO opportunity (workspace_id, stage_id, name, value, owner_id)
         SELECT $1::uuid, $2::uuid, 'seed-' || (g + $4::int), (g * 100)::numeric, $3::uuid
         FROM generate_series($5::int, ($5::int + $6::int - 1)) g`,
        [workspaceId, stageId, ownerId ?? null, offset, offset, size],
      );
    }
  } finally {
    client.release();
  }
}

export interface ApiResponseLike<T> {
  status: number;
  body: T;
}

export interface SubmittedJob {
  jobId: string;
  status: string;
  itemsCreated: number;
  replay: boolean;
  message?: string;
}

/** Submits a bulk move. The key defaults to a fresh uuid, as a UI would. */
export async function submitBulkMove(
  workspaceId: string,
  body: Record<string, unknown>,
  idempotencyKey?: string,
): Promise<{ status: number; body: SubmittedJob }> {
  const key = idempotencyKey ?? randomUUID();
  const response = await api<SubmittedJob>(BASE.transition, '/bulk-moves', {
    method: 'POST',
    workspaceId,
    body: JSON.stringify({ idempotencyKey: key, ...body }),
  });
  return { status: response.status, body: response.body };
}

/**
 * Records a transition attributed to a job, standing in for the worker.
 * Returns the transition id so a test can assert about that exact row rather
 * than searching for a match that another test may also have created.
 */
export async function recordJobTransition(
  workspaceId: string,
  jobId: string,
  opportunityId: string,
  fromStageId: string | null,
  toStageId: string,
): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO opportunity_transition
       (workspace_id, opportunity_id, from_stage_id, to_stage_id, job_id)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`,
    [workspaceId, opportunityId, fromStageId, toStageId, jobId],
  );
  await pool.query(
    `UPDATE bulk_job_item SET status = 'completed', completed_at = now()
     WHERE job_id = $1 AND opportunity_id = $2`,
    [jobId, opportunityId],
  );
  return rows[0]!.id;
}

/**
 * Writes a row with a sub-millisecond created_at. timestamptz stores
 * microseconds; a JS Date holds milliseconds. A cursor built by reading that
 * value into the client and sending it back is silently truncated, so the row
 * the cursor points at can satisfy the comparison and reappear on the next
 * page. The seeder only ever writes millisecond values, so without this the
 * bug stays invisible.
 */
export async function insertTransitionWithMicros(
  workspaceId: string,
  opportunityId: string,
  toStageId: string,
  micros: number,
  jobId?: string,
): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO opportunity_transition
       (workspace_id, opportunity_id, to_stage_id, job_id, created_at)
     VALUES ($1, $2, $3, $4::uuid, now() + ($5::text || ' microseconds')::interval)
     RETURNING id`,
    [workspaceId, opportunityId, toStageId, jobId ?? null, String(micros)],
  );
  return rows[0]!.id;
}

let n = 0;
function counter(): number {
  n += 1;
  return n;
}
