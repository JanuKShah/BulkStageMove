import { Pool } from 'pg';

export const DB_URL =
  process.env.DATABASE_URL ?? 'postgres://app:app_dev_password@localhost:5432/bulk_stage_move';

export const BASE = {
  workspace: 'http://localhost:3001',
  user: 'http://localhost:3002',
  stage: 'http://localhost:3003',
  opportunity: 'http://localhost:3004',
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

let n = 0;
function counter(): number {
  n += 1;
  return n;
}
