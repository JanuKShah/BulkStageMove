import { randomUUID } from 'node:crypto';
import { pool } from '../helpers';

/**
 * A workspace sized for the full brief: 50,000 opportunities moved in one job.
 *
 * Built with generate_series rather than the seeder so the fixture is
 * independent of the benchmark dataset and does not need a 500k seed to run.
 * Everything sits in one stage with a single forward rule, so every record in
 * the job is genuinely movable and the job is not refused for a reason that has
 * nothing to do with scale.
 */
export const HAPPY_FLOW_SIZE = 50_000;
const MARGIN = 2_000;

export interface HappyFlowWorkspace {
  workspaceId: string;
  /** The stage all records start in. */
  from: string;
  /** The stage the job moves them to. */
  to: string;
  /** Ids of the MARGIN records deliberately left out of the job's filter. */
  excluded: string[];
}

export async function createHappyFlowWorkspace(
  label = 'happyflow',
  size = HAPPY_FLOW_SIZE,
): Promise<HappyFlowWorkspace> {
  const workspaceId = await createWorkspace(`bench-${label}-${randomUUID().slice(0, 8)}`);
  const from = await createStage(workspaceId, 'Source');
  const to = await createStage(workspaceId, 'Target');

  await pool.query(
    `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
     VALUES ($1, $2, $3)`,
    [workspaceId, from, to],
  );

  // value splits the set: the job selects the low band, the margin is left above
  // it so the filter is doing real work rather than matching the whole table.
  await pool.query(
    `INSERT INTO opportunity (workspace_id, stage_id, name, value)
     SELECT $1, $2, 'op-' || g, g
       FROM generate_series(1, $3::int) g`,
    [workspaceId, from, size + MARGIN],
  );

  const { rows } = await pool.query<{ id: string }>(
    `SELECT id FROM opportunity
      WHERE workspace_id = $1 AND value > $2::int
      ORDER BY id`,
    [workspaceId, size],
  );
  return { workspaceId, from, to, excluded: rows.map((r) => r.id) };
}

async function createWorkspace(name: string): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    'INSERT INTO workspace (name) VALUES ($1) RETURNING id',
    [name],
  );
  return rows[0]!.id;
}

async function createStage(workspaceId: string, name: string): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    'INSERT INTO stage (workspace_id, name) VALUES ($1, $2) RETURNING id',
    [workspaceId, name],
  );
  return rows[0]!.id;
}

/**
 * The happy-path job's filter: everything in the low band, moved to `to`.
 *
 * Disjoint bands matter for any second job run against the same fixture. A
 * growing prefix would re-select records an earlier job already moved, and those
 * are then correctly refused as stale - which looks like a bug in the worker and
 * is actually the compare-and-swap doing its job.
 */
export function jobFilter(to: string): Record<string, unknown> {
  return { targetStageId: to, maxValue: HAPPY_FLOW_SIZE };
}

/** A value band that no other job in the sweep touches. */
export function bandFilter(to: string, from: number, to_: number): Record<string, unknown> {
  return { targetStageId: to, minValue: from, maxValue: to_ };
}

export interface Distribution {
  count: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  mean: number;
}

/** Nearest-rank percentile, so p99 is a value that was actually observed. */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(rank, sorted.length) - 1]!;
}

export function distribution(samples: number[]): Distribution {
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    count: sorted.length,
    min: sorted.length ? sorted[0]! : 0,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted.length ? sorted[sorted.length - 1]! : 0,
    mean: sorted.length ? sum / sorted.length : 0,
  };
}

export function ms(value: number): string {
  return value >= 1000 ? `${(value / 1000).toFixed(2)}s` : `${Math.round(value)}ms`;
}
