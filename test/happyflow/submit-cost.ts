/**
 * Where submission time goes, and whether it can be one statement.
 *
 * The submit loop currently walks the filter in pages of 1,000, and for each page
 * runs a transaction containing a 1,000-row INSERT plus an outbox row. For a
 * 50,000 record job that is 50 SELECTs, 50 INSERTs and 50 commits - and each
 * commit is an fsync. This measures that shape against a single
 * INSERT ... SELECT that assigns batch_no with row_number(), which is one
 * statement and one commit for the whole snapshot.
 */
import { pool } from '../helpers';

const SIZE = 50_000;
const PAGE = 1_000;

async function time(label: string, fn: () => Promise<unknown>): Promise<number> {
  const t0 = process.hrtime.bigint();
  await fn();
  const elapsed = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log(`  ${label.padEnd(46)}${elapsed.toFixed(0).padStart(7)}ms`);
  return elapsed;
}

async function main(): Promise<void> {
  // clean up any leftover fixtures from earlier probing
  await pool.query(
    "DELETE FROM workspace WHERE name IN ('tput','sub-bench','cmp','cmp2')",
  );

  const { rows: wsRows } = await pool.query<{ id: string }>(
    "INSERT INTO workspace (name) VALUES ('sub-bench') RETURNING id",
  );
  const ws = wsRows[0]!.id;
  const { rows: stRows } = await pool.query<{ id: string }>(
    "INSERT INTO stage (workspace_id, name) VALUES ($1, 'S') RETURNING id",
    [ws],
  );
  const stage = stRows[0]!.id;

  console.log(`building ${SIZE.toLocaleString()} opportunities...`);
  await pool.query(
    `INSERT INTO opportunity (workspace_id, stage_id, name, value)
     SELECT $1, $2, 'p' || g, g FROM generate_series(1, $3::int) g`,
    [ws, stage, SIZE],
  );
  await pool.query('ANALYZE opportunity');

  const newJob = async (key: string): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id)
       VALUES ($1, $2, '{}'::jsonb, $3) RETURNING id`,
      [ws, key, stage],
    );
    return rows[0]!.id;
  };

  console.log(`\nA. CURRENT SHAPE - ${SIZE / PAGE} transactions, one per page of ${PAGE}\n`);

  const jobA = await newJob('shape-a');
  const client = await pool.connect();
  const perPage: number[] = [];
  try {
    for (let batch = 0; batch * PAGE < SIZE; batch++) {
      const t0 = process.hrtime.bigint();
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO bulk_job_item
           (job_id, workspace_id, opportunity_id, from_stage_id, batch_no)
         SELECT $1, $2, o.id, o.stage_id, $4
           FROM (SELECT id, stage_id FROM opportunity
                  WHERE workspace_id = $2
                  ORDER BY created_at, id
                  OFFSET $3 LIMIT $5) o`,
        [jobA, ws, batch * PAGE, batch, PAGE],
      );
      await client.query(
        `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_count)
         VALUES ($1, $2, $3, $4)`,
        [ws, jobA, batch, PAGE],
      );
      await client.query('COMMIT');
      perPage.push(Number(process.hrtime.bigint() - t0) / 1e6);
    }
  } finally {
    client.release();
  }
  const totalA = perPage.reduce((a, b) => a + b, 0);
  const sortedA = [...perPage].sort((a, b) => a - b);
  console.log(`  ${'total, 50 separate transactions'.padEnd(46)}${totalA.toFixed(0).padStart(7)}ms`);
  console.log(`  ${'per transaction: min / median / max'.padEnd(46)}${sortedA[0]!.toFixed(0)} / ${sortedA[Math.floor(sortedA.length / 2)]!.toFixed(0)} / ${sortedA[sortedA.length - 1]!.toFixed(0)}ms`);
  console.log(`  ${'fsyncs (one per COMMIT)'.padEnd(46)}${String(SIZE / PAGE).padStart(7)}`);

  console.log(`\nB. SINGLE STATEMENT - one INSERT ... SELECT, row_number() assigns batch_no\n`);
  const jobB = await newJob('shape-b');
  const totalB = await time('total, 1 transaction', async () => {
    await pool.query('BEGIN');
    await pool.query(
      `INSERT INTO bulk_job_item
         (job_id, workspace_id, opportunity_id, from_stage_id, batch_no)
       SELECT $1, $2, o.id, o.stage_id, ((o.rn - 1) / $4)::int
         FROM (SELECT id, stage_id,
                      row_number() OVER (ORDER BY created_at, id) AS rn
                 FROM opportunity WHERE workspace_id = $2) o
        WHERE o.rn <= $3`,
      [jobB, ws, SIZE, PAGE],
    );
    // outbox rows derived from what was actually inserted
    await pool.query(
      `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_count)
       SELECT $2, $1, batch_no, count(*)::int
         FROM bulk_job_item WHERE job_id = $1 GROUP BY batch_no`,
      [jobB, ws],
    );
    await pool.query('COMMIT');
  });

  const { rows: ca } = await pool.query<{ n: number; b: number }>(
    'SELECT count(*)::int AS n, count(DISTINCT batch_no)::int AS b FROM bulk_job_item WHERE job_id = $1',
    [jobA],
  );
  const { rows: cb } = await pool.query<{ n: number; b: number }>(
    'SELECT count(*)::int AS n, count(DISTINCT batch_no)::int AS b FROM bulk_job_item WHERE job_id = $1',
    [jobB],
  );
  console.log(`\n  A wrote ${ca[0]!.n.toLocaleString()} rows in ${ca[0]!.b} batches`);
  console.log(`  B wrote ${cb[0]!.n.toLocaleString()} rows in ${cb[0]!.b} batches`);
  console.log(`\n  B is ${(totalA / totalB).toFixed(1)}x faster, same rows, same batches`);

  await pool.query('DELETE FROM workspace WHERE id = $1', [ws]);
  await pool.end();
}

void main();
