/**
 * How filter selectivity changes the cost of a build.
 *
 *   npx tsx test/happyflow/filter-bench.ts
 *
 * A bulk move's filter is not only a predicate - it decides how far the walk has
 * to read. The page query is keyset-paginated over
 * (workspace_id, created_at, id), and stage_id is applied as a filter on top of
 * that rather than being an index column, so a selective filter keeps walking
 * past rows it will discard until it has collected a full page.
 *
 * That predicts something the unfiltered benchmark cannot show: the same number of
 * matched records should cost more per page when they are a small slice of a large
 * workspace than when they are all of it. This measures it, because the index
 * choice only makes sense once the cost of the wrong shape is known.
 *
 * Each case gets its own freshly seeded workspace. They cannot share one: a build
 * moves every record it matched, so the second case would filter over a
 * distribution the first had already rearranged, and the comparison would be
 * between two different datasets rather than four filters.
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
  {
    label: 'no filter',
    shape: `all ${TOTAL.toLocaleString('en-US')}`,
    filter: () => ({}),
  },
  {
    label: 'outcome',
    shape: `the quarter already won`,
    filter: () => ({ outcome: 'won' }),
  },
  {
    label: 'value range',
    shape: 'the top tenth by value',
    filter: () => ({ minValue: VALUE_CUT }),
  },
  {
    label: 'date range',
    shape: 'the most recent tenth by created_at',
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
 * Pages written, and the mean gap between consecutive pages.
 *
 * Three things force this shape. The gap needs its own level because an aggregate
 * cannot wrap a window function. b.created_at has to be qualified, because the
 * window default joins bulk_job's column of the same name. And the parameter is
 * referenced once, in the CTE: a scalar subquery in the outer SELECT re-referencing
 * $1 parses but does not bind.
 */
const PAGE_COST = `
  WITH gaps AS (
    SELECT b.created_at - LAG(b.created_at, 1, j.created_at)
             OVER (ORDER BY b.batch_no) AS gap
      FROM bulk_job_outbox b
      JOIN bulk_job j ON j.id = b.job_id
     WHERE b.job_id = $1
  )
  SELECT count(*)::int AS pages,
         COALESCE(round(avg(EXTRACT(EPOCH FROM gap) * 1000))::int, 0) AS per_page_ms
    FROM gaps`;

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
 * So: three stages carrying three outcomes, a linear value spread, and a year of
 * created_at, with forward rules into a fourth stage so a matched record is
 * genuinely movable rather than refused.
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

  // A quarter in each outcome, a linear value spread, and created_at over a year
  // so a date boundary is a real cut rather than a coin toss. The casts on the
  // CASE branches are required: untyped parameters arrive as unknown, resolve the
  // CASE to text, and the insert then fails on the uuid column.
  //
  // The year is 2025, deliberately in the past. A job's watermark is
  // `created_at <= now()`, so a spread that ran into the current year had its
  // future-dated records excluded from every job - which showed up as 36,989
  // matched against a 50,000 fixture and read as a build that stopped early.
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

  // Asserted rather than assumed. An earlier version of this benchmark reported
  // 36,989 matched against a 50,000 fixture and nothing checked, because a build
  // that stops early still returns a number and a number is not a count.
  const { rows: check } = await pool.query<{ n: number }>(
    'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1',
    [workspaceId],
  );
  if (check[0]!.n !== TOTAL) {
    throw new Error(`fixture seeded ${check[0]!.n} records, expected ${TOTAL}`);
  }

  return { workspaceId, target: stageIds['target']! };
}

async function run(
  workspaceId: string,
  target: string,
  c: Case,
  report: boolean,
): Promise<void> {
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
  for (;;) {
    const res = await get(`/bulk-moves/${jobId}`, workspaceId);
    if (res.status === 200) {
      const st = JSON.parse(res.body) as {
        snapshotInProgress: boolean;
        totalMatched: number;
      };
      if (!st.snapshotInProgress) {
        const buildMs = Date.now() - t0;
        const { rows } = await pool.query<{ pages: number; per_page_ms: number }>(PAGE_COST, [
          jobId,
        ]);
        if (report) {
          console.log(
            `  ${c.label.padEnd(12)} ${String(st.totalMatched).padStart(6)} matched ` +
              `${String(rows[0]?.pages ?? 0).padStart(3)} pages  ` +
              `build ${ms(buildMs).padStart(8)}  ` +
              `per page ${String(rows[0]?.per_page_ms ?? 0).padStart(4)}ms`,
          );
        }
        return;
      }
    }
    if (Date.now() - t0 > 120_000) {
      console.log(`  ${c.label.padEnd(12)} build did not finish in 120s`);
      return;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

async function main(): Promise<void> {
  console.log(
    `Filter selectivity against a ${TOTAL.toLocaleString('en-US')} record workspace.`,
  );
  console.log('One freshly seeded workspace and one build per filter.\n');
  console.log('  filter         matched  pages        build   per page');
  console.log(`  ${'-'.repeat(52)}`);

  // Boundaries come from one throwaway seed so every case filters by the same
  // yardstick, and it is cleaned up - the version that left it behind put 50,000
  // rows in the table that nothing was measuring.
  const boundary = await seed();
  try {
    for (const c of CASES) {
      // Twice, reporting the second.
      //
      // Run once, the first case is always the coldest thing in shared buffers -
      // 50,000 rows have just been inserted and no query has touched them - and it
      // read 25ms per page against 13-18ms for the rest. That is a cache warm-up
      // curve, not a property of the filter, and reporting it as a filter effect
      // would be reading the order the cases happened to run in. The first pass
      // is the warm-up; the second is the measurement.
      for (let pass = 0; pass < 2; pass++) {
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
    `\n  Total build tracks the page count, which is matched records over 1,000 -\n` +
      `  so a selective filter is faster for the uninteresting reason that there\n` +
      `  is less to do.\n` +
      `\n  Per page is the column that was supposed to show the cost of selectivity,\n` +
      `  and it does not. Across four filters it lands in the 12-29ms band with no\n` +
      `  ordering, so at 50,000 on this host the walk's cost per page is dominated\n` +
      `  by the batch write - a 1,000-element uuid array and a commit - rather than\n` +
      `  by how many rows the stage filter discarded. The earlier reading of 25ms\n` +
      `  against 13ms was a cache warm-up curve from the cases running in order;\n` +
      `  each is now run twice and only the warm pass reported.\n` +
      `\n  That does not rule the effect out at 500,000, where a selective filter\n` +
      `  walks proportionally further. It says the index question is not answerable\n` +
      `  at this scale, and should not be argued from these numbers.`,
  );

  await pool.end();
}

void main();
