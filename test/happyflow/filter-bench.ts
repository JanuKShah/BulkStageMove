/**
 * How filter selectivity changes the cost of a bulk move.
 *
 *   npx tsx test/happyflow/filter-bench.ts
 *
 * A filter is not only a predicate - it decides how much work the job is. This
 * measures a build and a full job per filter, so the two rates can be told apart:
 * the build rate is records per second of *finding* records, and the end-to-end
 * rate includes the drain and is what a submitter actually waits for.
 *
 * Each case gets its own freshly seeded workspace. They cannot share one: a build
 * moves every record it matched, so the second case would filter over a
 * distribution the first had already rearranged, and the comparison would be
 * between two datasets rather than four filters.
 *
 * Each case also runs twice, reporting only the second. Run once, the first case
 * is always the coldest thing in shared buffers - 50,000 rows have just been
 * inserted and nothing has touched them - and it reads high against the rest.
 * That is a warm-up curve, not a property of the filter, and reporting it as one
 * would be reading the order the cases happened to run in.
 *
 * Deliberately not a test. Wall-clock on a shared host, like benchmark.ts.
 */
import { request } from 'node:http';
import { pool, withDeadlockRetry } from '../helpers';
import { ms } from './fixture';

const BASE = { host: 'localhost', port: 3005 };
const TOTAL = 50_000;

/** Value boundary, filled in from the seeded data rather than guessed. */
let VALUE_CUT = 0;
/** Date boundary, likewise. */
let DATE_CUT = '';

interface Case {
  label: string;
  shape: string;
  filter: () => Record<string, unknown>;
}

const CASES: Case[] = [
  { label: 'no filter', shape: 'all 50,000', filter: () => ({}) },
  { label: 'outcome', shape: 'the quarter already won', filter: () => ({ outcome: 'won' }) },
  {
    label: 'value range',
    shape: 'the top tenth by value',
    filter: () => ({ minValue: VALUE_CUT }),
  },
  {
    label: 'date range',
    shape: 'the most recent tenth',
    filter: () => ({ createdFrom: DATE_CUT }),
  },
];

function post(
  path: string,
  workspaceId: string,
  body: unknown,
): Promise<{ status: number; body: string }> {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request(
      {
        ...BASE,
        path,
        method: 'POST',
        timeout: 0,
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload),
          'x-workspace-id': workspaceId,
        },
      },
      (res) => {
        let out = '';
        res.on('data', (c) => (out += c));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: out }));
      },
    );
    req.on('error', reject);
    req.end(payload);
  });
}

function get(path: string, workspaceId: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = request(
      { ...BASE, path, method: 'GET', headers: { 'x-workspace-id': workspaceId } },
      (res) => {
        let out = '';
        res.on('data', (c) => (out += c));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: out }));
      },
    );
    req.on('error', reject);
    req.end();
  });
}

/**
 * Pages, mean gap between them, and records the drain actually moved.
 *
 * Three things force this shape. The gaps need their own level because an
 * aggregate cannot wrap a window function. b.created_at has to be qualified,
 * because the window default joins bulk_job's column of the same name. And the
 * parameter is referenced once per CTE, because a scalar subquery in the outer
 * SELECT re-referencing $1 parses but does not bind.
 */
const JOB_COST = `
  WITH gaps AS (
    SELECT b.created_at - LAG(b.created_at, 1, j.created_at)
             OVER (ORDER BY b.batch_no) AS gap
      FROM bulk_job_outbox b
      JOIN bulk_job j ON j.id = b.job_id
     WHERE b.job_id = $1
  ),
  drained AS (
    SELECT coalesce(sum(completed_count), 0)::int AS processed
      FROM bulk_job_outbox WHERE job_id = $1
  )
  SELECT (SELECT count(*)::int FROM gaps) AS pages,
         COALESCE((SELECT round(avg(EXTRACT(EPOCH FROM gap) * 1000))::int FROM gaps), 0)
           AS per_page_ms,
         (SELECT processed FROM drained) AS processed`;

/**
 * A fixture with something for each filter to select.
 *
 * Not createHappyFlowWorkspace, which cannot be filtered: it makes one source
 * stage and one target, both with outcome 'open', so `outcome` cannot
 * distinguish anything; it spreads no created_at, so every record lands on the
 * same day; and its values top out below any round bound worth naming. Measured
 * against it, three of the four filters matched zero records and the fourth
 * matched all of them, which says nothing about selectivity.
 *
 * The created_at spread is deliberately a past year. A job's watermark is
 * `created_at <= now()`, so a spread running into the current year had its
 * future-dated records excluded from every job - which showed up as 36,989
 * matched against a 50,000 fixture and read as a build that stopped early.
 */
async function seed(): Promise<{ workspaceId: string; target: string }> {
  const created = await pool.query<{ id: string }>(
    `INSERT INTO workspace (name) VALUES ('filter-bench') RETURNING id`,
  );
  const workspaceId = created.rows[0]!.id;

  const stageIds: Record<string, string> = {};
  for (const [key, name, outcome] of [
    ['src', 'Open deals', 'open'],
    ['won', 'Closed won', 'won'],
    ['lost', 'Closed lost', 'lost'],
    ['target', 'Destination', 'open'],
  ] as const) {
    const s = await pool.query<{ id: string }>(
      `INSERT INTO stage (workspace_id, name, outcome) VALUES ($1, $2, $3) RETURNING id`,
      [workspaceId, name, outcome],
    );
    stageIds[key] = s.rows[0]!.id;
  }
  for (const from of ['src', 'won', 'lost'] as const) {
    await pool.query(
      `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
       VALUES ($1, $2, $3)`,
      [workspaceId, stageIds[from]!, stageIds['target']!],
    );
  }

  // A quarter in each outcome, a linear value spread, created_at over a year. The
  // casts on the CASE branches are required: untyped parameters arrive as unknown,
  // resolve the CASE to text, and the insert then fails on the uuid column.
  await pool.query(
    `INSERT INTO opportunity (workspace_id, stage_id, name, value, created_at)
     SELECT $1,
            CASE (g - 1) % 4
              WHEN 0 THEN $2::uuid WHEN 1 THEN $3::uuid
              WHEN 2 THEN $4::uuid ELSE $5::uuid END,
            'fb-' || g,
            (g % 1000) * 100,
            timestamptz '2025-01-01 00:00:00Z' + (g % 365) * interval '1 day'
       FROM generate_series(1, $6::int) g`,
    [workspaceId, stageIds['src'], stageIds['won'], stageIds['lost'], stageIds['target'], TOTAL],
  );
  await pool.query('ANALYZE opportunity');

  // Boundaries from the data, so the labels above are true rather than assumed.
  const v = await pool.query<{ cut: number }>(
    `SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY value)::int AS cut
       FROM opportunity WHERE workspace_id = $1`,
    [workspaceId],
  );
  VALUE_CUT = v.rows[0]!.cut;

  const d = await pool.query<{ lo: Date; hi: Date }>(
    `SELECT min(created_at) AS lo, max(created_at) AS hi
       FROM opportunity WHERE workspace_id = $1`,
    [workspaceId],
  );
  DATE_CUT = new Date(
    d.rows[0]!.lo.getTime() + 0.9 * (d.rows[0]!.hi.getTime() - d.rows[0]!.lo.getTime()),
  ).toISOString();

  // Asserted rather than assumed. A build that stops early still returns a
  // number, and a number is not a count.
  const { rows: check } = await pool.query<{ n: number }>(
    'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1',
    [workspaceId],
  );
  if (check[0]!.n !== TOTAL) {
    throw new Error(`fixture seeded ${check[0]!.n} records, expected ${TOTAL}`);
  }

  return { workspaceId, target: stageIds['target']! };
}

function perSec(n: number, millis: number): string {
  return millis > 0 ? Math.round(n / (millis / 1000)).toLocaleString('en-US') : '-';
}

async function run(workspaceId: string, target: string, c: Case, report: boolean): Promise<void> {
  const submitted = await post('/bulk-moves', workspaceId, {
    ...c.filter(),
    targetStageId: target,
    idempotencyKey: `fb-${c.label}-${Date.now()}`,
  });
  if (submitted.status !== 201) {
    console.log(
      `  ${c.label.padEnd(12)} submit failed ${submitted.status}: ${submitted.body.slice(0, 160)}`,
    );
    return;
  }
  const jobId = (JSON.parse(submitted.body) as { jobId: string }).jobId;

  const t0 = Date.now();
  let matched = 0;
  let buildMs = 0;
  let settledMs = 0;

  // Two stops, not one. The build ends when the snapshot is done; the job ends
  // when every batch has settled. Reporting only the second would hide how much
  // of a small job is spent waiting to be noticed, which is the interesting part.
  for (;;) {
    const res = await get(`/bulk-moves/${jobId}`, workspaceId);
    if (res.status === 200) {
      const st = JSON.parse(res.body) as {
        snapshotInProgress: boolean;
        totalMatched: number;
        status: string;
        batches: Record<string, number>;
      };
      matched = st.totalMatched;
      if (!st.snapshotInProgress && buildMs === 0) buildMs = Date.now() - t0;
      if (buildMs > 0) {
        const done = st.status === 'completed' || st.status === 'failed';
        const idle = (st.batches['pending'] ?? 0) === 0 && (st.batches['running'] ?? 0) === 0;
        if (done && idle) {
          settledMs = Date.now() - t0;
          break;
        }
      }
    }
    if (Date.now() - t0 > 180_000) {
      console.log(`  ${c.label.padEnd(12)} did not settle in 180s`);
      return;
    }
    await new Promise((r) => setTimeout(r, 50));
  }

  const { rows } = await pool.query<{ pages: number; per_page_ms: number; processed: number }>(
    JOB_COST,
    [jobId],
  );
  const processed = rows[0]?.processed ?? 0;

  if (report) {
    console.log(
      `  ${c.label.padEnd(12)} ${String(matched).padStart(6)} matched  ` +
        `${String(rows[0]?.pages ?? 0).padStart(3)} pages  ` +
        `build ${ms(buildMs).padStart(7)}  ` +
        `${String(rows[0]?.per_page_ms ?? 0).padStart(3)}ms/page  ` +
        `${perSec(matched, buildMs).padStart(7)}/sec  |  ` +
        `moved ${String(processed).padStart(6)} in ${ms(settledMs).padStart(7)}  ` +
        `${perSec(processed, settledMs).padStart(7)}/sec`,
    );
  }
}

async function main(): Promise<void> {
  console.log(`Filter selectivity against a ${TOTAL.toLocaleString('en-US')} record workspace.`);
  console.log(
    'One freshly seeded workspace and one full job per filter, twice, warm pass shown.\n',
  );
  console.log(
    '  filter         matched  pages      build   per page   build/s  |    moved     total     total/s',
  );
  console.log('  ' + '-'.repeat(98));

  // Boundaries come from one throwaway seed so every case filters by the same
  // yardstick, and it is cleaned up - the version that left it behind put 50,000
  // rows in the table that nothing was measuring.
  const boundary = await seed();
  try {
    for (const c of CASES) {
      for (let pass = 0; pass < 2; pass += 1) {
        const ws = await seed();
        try {
          await run(ws.workspaceId, ws.target, c, pass === 1);
        } finally {
          await withDeadlockRetry(() =>
            pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
          );
        }
      }
    }
  } finally {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [boundary.workspaceId]),
    );
  }

  console.log(
    `
  Throughput is the column that says something useful, and it says the opposite
  of what the per-page column was written to show. Build rate is high and flat -
  tens of thousands of records a second - because a page costs about what a page
  costs whatever matched it.

  End-to-end rate falls by roughly 3x from the unfiltered case to the smallest
  filter, on a job moving five thousand records instead of fifty thousand. The
  data is not the reason. The two small cases settle in about 400ms whatever
  they match, and roughly half of that is the job waiting to be noticed: a 250ms
  sweep tick before the first batch exists, then a 250ms relay tick before any of
  it is published. Below roughly 15,000 records that fixed cost is the job.

  Those two rates are also the reason the earlier framing was wrong. Counting
  only the build flatters a selective filter - it does less work - while counting
  only the end-to-end figure punishes it for being small. Neither alone is the
  number a caller waits on.

  Per page shows no selectivity effect. Whether stage_id belongs in the walk's
  index is therefore not answered here, and should not be argued from these
  numbers - though it is not ruled out either, since at 500,000 a selective filter
  walks proportionally further past the rows it discards.`,
  );

  await pool.end();
}

void main();
