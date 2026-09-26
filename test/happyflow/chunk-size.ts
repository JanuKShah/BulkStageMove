/**
 * Picks the snapshot chunk size.
 *
 * The submit loop runs one transaction per chunk, so a transaction is bounded by
 * the chunk size rather than by the match count. Collapsing it into a single
 * unbounded INSERT ... SELECT is faster per commit but sizes the transaction by
 * how many opportunities matched, which is the property worth keeping.
 *
 * This measures the same one-statement insert at several chunk sizes, so the
 * knee is chosen from data rather than guessed. batch_no is (rn - 1) / 1000 in
 * every variant, so the batch contract is identical to the paged version.
 *
 * One workspace and one 50,000-row fixture are built once and reused, and each
 * variant's items are deleted before the next, so every variant starts from the
 * same opportunity table and an empty bulk_job_item. Rebuilding the fixture per
 * variant would grow the table 300,000 rows across the run and penalise whichever
 * variants went last.
 *
 * A watermark (created_at <= $snapshot) is bound from the application rather than
 * using now(), because each chunk is its own transaction and now() is transaction
 * start time - it would advance per chunk and re-open the set the snapshot is
 * meant to have closed.
 */
import { pool } from '../helpers';

const SIZE = 50_000;
const BATCH = 1_000;
const CHUNKS = [1_000, 2_000, 5_000, 10_000, 25_000, 50_000];

interface Cursor {
  created_at: Date;
  id: string;
}

/** 40P01 is a deadlock; it is transient, so the loser of a race just retries. */
async function withRetry<T>(fn: () => Promise<T>, attempts = 6): Promise<T> {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      const code = (err as { code?: string }).code;
      if (code !== '40P01' || i >= attempts) throw err;
      await new Promise((r) => setTimeout(r, 300 * i));
    }
  }
}

async function sweep(
  ws: string,
  stage: string,
  chunkSize: number,
): Promise<{ total: number; worst: number; rows: number }> {
  const { rows: jRows } = await pool.query<{ id: string }>(
    `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id)
     VALUES ($1, $2, '{}'::jsonb, $3) RETURNING id`,
    [ws, `chunk-${chunkSize}`, stage],
  );
  const job = jRows[0]!.id;

  // Captured once, before any chunk runs.
  const { rows: wRows } = await pool.query<{ t: Date }>('SELECT now() AS t');
  const watermark = wRows[0]!.t;

  let cursor: Cursor | null = null;
  let offset = 0;
  const perChunk: number[] = [];
  const t0 = process.hrtime.bigint();

  const client = await pool.connect();
  try {
    for (;;) {
      const params: unknown[] = [job, ws, offset, watermark, chunkSize, BATCH];
      let where = 'workspace_id = $2 AND created_at <= $4';
      if (cursor) {
        params.push(cursor.id);
        where += ` AND (created_at, id) > (
          SELECT created_at, id FROM opportunity WHERE id = $${params.length} AND workspace_id = $2)`;
      }
      const t1 = process.hrtime.bigint();
      await client.query('BEGIN');
      // The window is computed over the inner LIMITed set, so it costs O(chunk)
      // rather than O(remaining). The running offset is added back to make rn
      // global, which is what keeps batch_no correct across chunk boundaries.
      const ins = await client.query<{ id: string }>(
        `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id, from_stage_id, batch_no)
         SELECT $1, $2, o.id, o.stage_id, (($3 + o.rn - 1) / $6)::int
           FROM (SELECT id, stage_id, row_number() OVER (ORDER BY created_at, id) AS rn
                   FROM (SELECT id, stage_id, created_at FROM opportunity
                          WHERE ${where}
                          ORDER BY created_at, id
                          LIMIT $5) lim) o
         ON CONFLICT (job_id, opportunity_id) DO NOTHING
         RETURNING opportunity_id AS id`,
        params,
      );
      const count = ins.rowCount ?? 0;
      if (count > 0) {
        const lo = Math.floor(offset / BATCH);
        const hi = Math.floor((offset + chunkSize - 1) / BATCH);
        await client.query(
          `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_count)
           SELECT $2, $1, batch_no, count(*)::int FROM bulk_job_item
            WHERE job_id = $1 AND batch_no BETWEEN $3 AND $4
            GROUP BY batch_no
           ON CONFLICT (job_id, batch_no) DO NOTHING`,
          [job, ws, lo, hi],
        );
        await client.query(
          'UPDATE bulk_job SET total_matched = total_matched + $3, updated_at = now() WHERE id = $1 AND workspace_id = $2',
          [job, ws, count],
        );
      }
      await client.query('COMMIT');
      perChunk.push(Number(process.hrtime.bigint() - t1) / 1e6);

      if (count < chunkSize) break;
      // The cursor is read back from Postgres inside the transaction rather than
      // reused from a JS Date: created_at is timestamptz with microseconds, so a
      // round-tripped Date is truncated and the next chunk re-reads this one.
      const lastRow = await client.query<{ id: string; created_at: Date }>(
        'SELECT id, created_at FROM opportunity WHERE id = $1 AND workspace_id = $2',
        [ins.rows[count - 1]!.id, ws],
      );
      cursor = lastRow.rows[0]!;
      offset += count;
    }
  } finally {
    client.release();
  }
  const total = Number(process.hrtime.bigint() - t0) / 1e6;

  const { rows: v } = await pool.query<{ n: number; b: number }>(
    'SELECT count(*)::int AS n, count(DISTINCT batch_no)::int AS b FROM bulk_job_item WHERE job_id = $1',
    [job],
  );
  if (v[0]!.n !== SIZE) throw new Error(`chunk ${chunkSize}: wrote ${v[0]!.n}, expected ${SIZE}`);
  if (v[0]!.b !== Math.ceil(SIZE / BATCH)) {
    throw new Error(`chunk ${chunkSize}: ${v[0]!.b} batches, expected ${Math.ceil(SIZE / BATCH)}`);
  }

  console.log(
    `  ${String(chunkSize).padStart(6)} | ${String(perChunk.length - 1).padStart(3)} | ` +
      `${total.toFixed(0).padStart(6)}ms | ${Math.max(...perChunk).toFixed(0).padStart(5)}ms`,
  );

  // Clear this variant so the next one starts from an empty bulk_job_item.
  await withRetry(() => pool.query('DELETE FROM bulk_job_item WHERE job_id = $1', [job]));
  await withRetry(() => pool.query('DELETE FROM bulk_job_outbox WHERE job_id = $1', [job]));
  await withRetry(() => pool.query('DELETE FROM bulk_job WHERE id = $1', [job]));

  return { total, worst: Math.max(...perChunk), rows: SIZE };
}

async function main(): Promise<void> {
  const { rows: wsRows } = await pool.query<{ id: string }>(
    "INSERT INTO workspace (name) VALUES ('chunk-sweep') RETURNING id",
  );
  const ws = wsRows[0]!.id;
  const { rows: stRows } = await pool.query<{ id: string }>(
    "INSERT INTO stage (workspace_id, name) VALUES ($1, 'S') RETURNING id",
    [ws],
  );
  const stage = stRows[0]!.id;

  console.log(`building ${SIZE.toLocaleString()} opportunities (reused by every variant)...`);
  await pool.query(
    `INSERT INTO opportunity (workspace_id, stage_id, name, value)
     SELECT $1, $2, 'p' || g, g FROM generate_series(1, $3::int) g`,
    [ws, stage, SIZE],
  );
  await pool.query('ANALYZE opportunity');

  console.log(`\n${SIZE.toLocaleString()} records, batch=${BATCH}, one INSERT...SELECT per chunk\n`);
  console.log('  chunk | txns |   total | worst chunk');

  const results: { chunk: number; total: number; worst: number }[] = [];
  for (const c of CHUNKS) {
    const r = await sweep(ws, stage, c);
    results.push({ chunk: c, total: r.total, worst: r.worst });
  }

  const base = results[0]!.total;
  const oneStmt = results.find((r) => r.chunk === SIZE);
  console.log('\n  speedup vs 1,000-row chunks:');
  for (const r of results) {
    console.log(`    ${String(r.chunk).padStart(6)}  ${(base / r.total).toFixed(2)}x`);
  }
  if (oneStmt) {
    console.log(
      `\n  unbounded single statement: ${oneStmt.total.toFixed(0)}ms, ` +
        `${(base / oneStmt.total).toFixed(2)}x - but its transaction is the whole match count`,
    );
  }
  await withRetry(() => pool.query('DELETE FROM workspace WHERE id = $1', [ws]));
  await pool.end();
}

void main();
