/**
 * Seed data generator. Connects straight to Postgres - this is a fixture
 * loader, not a business operation, and pushing 500k rows through the API would
 * test the wrong layer.
 *
 * What --seed makes reproducible:
 *   deterministic  names, values, statuses, stage distribution, owners
 *   not identical   ids (gen_random_uuid) and created_at (anchored to the wall
 *                   clock, so the dataset always spans the last 18 months)
 * Benchmarks depend on the distribution being stable, not on byte-identical
 * rows, so this is sufficient - but it is not the same claim.
 *
 *   npm run seed              small dataset (fast, for the one-command flow)
 *   npm run seed:large        full benchmark dataset
 *   npm run seed -- --scale=large --seed=42
 */
import { Pool } from 'pg';
import { databaseUrl } from './env';

const STAGES: ReadonlyArray<{ name: string; weight: number; outcome: Outcome }> = [
  { name: 'New Lead', weight: 22, outcome: 'open' },
  { name: 'Contacted', weight: 18, outcome: 'open' },
  { name: 'Qualified', weight: 14, outcome: 'open' },
  { name: 'Proposal Sent', weight: 11, outcome: 'open' },
  { name: 'Negotiation', weight: 9, outcome: 'open' },
  { name: 'Contract Sent', weight: 7, outcome: 'open' },
  { name: 'Legal Review', weight: 5, outcome: 'open' },
  { name: 'Payment Pending', weight: 4, outcome: 'open' },
  { name: 'Onboarding', weight: 3, outcome: 'open' },
  { name: 'Closed Won', weight: 3, outcome: 'won' },
  { name: 'Closed Lost', weight: 2.5, outcome: 'lost' },
  { name: 'Abandoned', weight: 1.5, outcome: 'abandoned' },
];

type Outcome = 'open' | 'won' | 'lost' | 'abandoned';

const MONTHS_OF_HISTORY = 18;
const USERS_PER_WORKSPACE = 12;
const BATCH = 5_000;
const SMALL_WORKSPACES = 5;

const SCALES = {
  small: { largeWorkspace: 10_000, smallWorkspace: 1_000, seed: 1 },
  large: { largeWorkspace: 500_000, smallWorkspace: 5_000, seed: 1 },
} as const;

type ScaleName = keyof typeof SCALES;

function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function parseArgs(): { scale: ScaleName; seed: number } {
  const argv = process.argv.slice(2);
  const scaleArg = argv.find((a) => a.startsWith('--scale='))?.split('=')[1];
  const seedArg = argv.find((a) => a.startsWith('--seed='))?.split('=')[1];
  const scale = (scaleArg ?? 'small') as ScaleName;
  if (!(scale in SCALES)) {
    throw new Error(`unknown scale "${scale}". use small or large.`);
  }
  return { scale, seed: seedArg ? Number(seedArg) : SCALES[scale].seed };
}

/** Cumulative weights, so a stage is picked by binary search over the total. */
function buildCumulative(): number[] {
  const total = STAGES.reduce((sum, s) => sum + s.weight, 0);
  let acc = 0;
  return STAGES.map((s) => (acc += s.weight / total));
}

function pickStageIndex(rand: () => number, cumulative: readonly number[]): number {
  const roll = rand();
  for (let i = 0; i < cumulative.length; i++) {
    const edge = cumulative[i];
    if (edge !== undefined && roll < edge) return i;
  }
  return cumulative.length - 1;
}

async function main(): Promise<void> {
  const connectionString = databaseUrl();

  const { scale, seed } = parseArgs();
  const { largeWorkspace, smallWorkspace } = SCALES[scale];
  const rand = mulberry32(seed);
  const cumulative = buildCumulative();
  const pool = new Pool({ connectionString, max: 4 });
  const started = Date.now();

  try {
    await pool.query(
      'TRUNCATE opportunity, stage_transition_rule, stage, app_user, workspace CASCADE',
    );

    const historyMs = MONTHS_OF_HISTORY * 30 * 24 * 60 * 60 * 1000;
    const now = Date.now();

    // One INSERT ... SELECT so all 12 stages land in a single round trip.
    const insertWorkspace = async (name: string, count: number): Promise<WorkspaceSeed> => {
      const { rows } = await pool.query<{ id: string }>(
        'INSERT INTO workspace (name) VALUES ($1) RETURNING id',
        [name],
      );
      const id = rows[0]?.id;
      if (!id) throw new Error('failed to create workspace');

      const userIds = await Promise.all(
        Array.from({ length: USERS_PER_WORKSPACE }, async (_, i) => {
          const r = await pool.query<{ id: string }>(
            'INSERT INTO app_user (workspace_id, name, email) VALUES ($1, $2, $3) RETURNING id',
            [id, `User ${i + 1}`, `user-${i + 1}@${name.toLowerCase().replace(/\s+/g, '-')}.test`],
          );
          return r.rows[0]!.id;
        }),
      );

      const stageIds: string[] = [];
      for (const stage of STAGES) {
        const r = await pool.query<{ id: string }>(
          'INSERT INTO stage (workspace_id, name, outcome) VALUES ($1, $2, $3) RETURNING id',
          [id, stage.name, stage.outcome],
        );
        stageIds.push(r.rows[0]!.id);
      }

      // Forward moves always allowed; backward only up to two steps. This leaves
      // some moves deliberately absent, so the "non-transitionable" path is
      // reachable in tests instead of every move being valid.
      const fromIds: string[] = [];
      const toIds: string[] = [];
      for (let i = 0; i < stageIds.length; i++) {
        for (let j = 0; j < stageIds.length; j++) {
          if (i === j) continue;
          if (j > i || j >= i - 2) {
            fromIds.push(stageIds[i]!);
            toIds.push(stageIds[j]!);
          }
        }
      }
      await pool.query(
        `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
         SELECT $1, f, t FROM unnest($2::uuid[], $3::uuid[]) AS r(f, t)
         ON CONFLICT DO NOTHING`,
        [id, fromIds, toIds],
      );

      let written = 0;
      while (written < count) {
        const size = Math.min(BATCH, count - written);
        const stageCol: string[] = [];
        const nameCol: string[] = [];
        const valueCol: string[] = [];
        const ownerCol: string[] = [];
        const createdCol: Date[] = [];

        for (let i = 0; i < size; i++) {
          const stageIdx = pickStageIndex(rand, cumulative);
          stageCol.push(stageIds[stageIdx]!);
          nameCol.push(`opportunity-${written + i}`);
          valueCol.push((rand() * 250_000).toFixed(2));
          ownerCol.push(userIds[Math.floor(rand() * userIds.length)]!);
          createdCol.push(new Date(now - Math.floor(rand() * historyMs)));
        }

        await pool.query(
          `INSERT INTO opportunity
             (workspace_id, stage_id, name, value, owner_id, created_at)
           SELECT $1, s, n, v, o, c
           FROM unnest($2::uuid[], $3::text[], $4::numeric[], $5::uuid[], $6::timestamptz[])
             AS r(s, n, v, o, c)`,
          [id, stageCol, nameCol, valueCol, ownerCol, createdCol],
        );
        written += size;
      }

      return { id, name, count };
    };

    const seeded: WorkspaceSeed[] = [];
    seeded.push(await insertWorkspace('large', largeWorkspace));
    for (let i = 1; i <= SMALL_WORKSPACES; i++) {
      seeded.push(await insertWorkspace(`small-${i}`, smallWorkspace));
    }

    const { rows: totals } = await pool.query<{ stage: string; n: string; value: string }>(
      `SELECT s.name AS stage, count(*) AS n, coalesce(sum(o.value),0) AS value
       FROM opportunity o JOIN stage s ON s.id = o.stage_id
       WHERE o.workspace_id = $1 GROUP BY s.name ORDER BY count(*) DESC`,
      [seeded[0]!.id],
    );

    const { rows: outcomes } = await pool.query<{ outcome: string; n: string }>(
      `SELECT s.outcome, count(*) AS n
       FROM opportunity o JOIN stage s ON s.id = o.stage_id
       WHERE o.workspace_id = $1 GROUP BY s.outcome ORDER BY n DESC`,
      [seeded[0]!.id],
    );

    const elapsed = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`scale=${scale} seed=${seed} completed in ${elapsed}s\n`);

    for (const w of seeded) {
      console.log(`  ${w.name.padEnd(10)} ${w.count.toLocaleString().padStart(9)} opportunities`);
    }

    console.log(`\nlarge workspace distribution (uneven by design):`);
    for (const row of totals) {
      const pct = (Number(row.n) / largeWorkspace) * 100;
      console.log(
        `  ${row.stage.padEnd(16)} ${Number(row.n).toLocaleString().padStart(8)}  ${pct.toFixed(1).padStart(5)}%`,
      );
    }

    console.log(`\noutcome spread in large workspace (derived from stage, not stored per row):`);
    for (const row of outcomes) {
      console.log(`  ${row.outcome.padEnd(16)} ${Number(row.n).toLocaleString().padStart(8)}`);
    }

    console.log(`\nworkspace ids:`);
    for (const w of seeded) console.log(`  ${w.name.padEnd(10)} ${w.id}`);
  } finally {
    await pool.end();
  }
}

interface WorkspaceSeed {
  id: string;
  name: string;
  count: number;
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
