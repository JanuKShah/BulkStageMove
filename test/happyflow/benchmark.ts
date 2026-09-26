/**
 * Happy-flow benchmark: one 50,000-opportunity bulk move, end to end.
 *
 * Run it deliberately; do not assert on it. Wall-clock numbers depend on the
 * machine, so a latency assertion in a test fails on a loaded CI box and passes
 * on a laptop. The correctness of the same flow is asserted in
 * bulk-happy-flow.spec.ts; this only measures.
 *
 *   npm run bench
 */
import { execSync } from 'node:child_process';
import { request } from 'node:http';
import { pool } from '../helpers';
import { createHappyFlowWorkspace, distribution, jobFilter, ms, HAPPY_FLOW_SIZE } from './fixture';

const BASE = { host: 'localhost', port: 3005 };

/**
 * node:http rather than fetch. undici applies its own headers timeout, which
 * turns "submission is slow" into a client-side failure and loses the
 * measurement entirely. A latency harness that cannot observe a slow request is
 * not measuring latency.
 */
function post(
  path: string,
  workspaceId: string,
  body: unknown,
): Promise<{ status: number; body: string; ms: number }> {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const started = process.hrtime.bigint();
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
        res.on('end', () =>
          resolve({
            status: res.statusCode ?? 0,
            body: out,
            ms: Number(process.hrtime.bigint() - started) / 1e6,
          }),
        );
      },
    );
    req.on('error', reject);
    req.end(payload);
  });
}

function line(label: string, value: string): void {
  console.log(`  ${label.padEnd(38)}${value}`);
}

async function main(): Promise<void> {
  console.log(`building a ${HAPPY_FLOW_SIZE.toLocaleString()} opportunity fixture...`);
  const buildStart = Date.now();
  const ws = await createHappyFlowWorkspace('bench');
  line('fixture built in', ms(Date.now() - buildStart));

  console.log('\nsubmitting one bulk move over the whole set...');
  const submitted = await post('/bulk-moves', ws.workspaceId, {
    idempotencyKey: `bench-${Date.now()}`,
    ...jobFilter(ws.to),
  });
  line('http status', String(submitted.status));
  if (submitted.status !== 201) {
    console.log(`  body: ${submitted.body.slice(0, 300)}`);
    await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
    await pool.end();
    return;
  }
  const created = JSON.parse(submitted.body) as { jobId: string; itemsCreated: number };
  line('items created', created.itemsCreated.toLocaleString());
  line('SUBMISSION LATENCY (returns job id)', ms(submitted.ms));
  const jobId = created.jobId;

  console.log('\nwaiting for the worker to drain every batch...');
  const t0 = Date.now();
  let final = { status: 'timeout', processed_count: 0, failed_count: 0 };
  for (;;) {
    const { rows } = await pool.query<{
      status: string;
      processed_count: number;
      failed_count: number;
    }>('SELECT status, processed_count, failed_count FROM bulk_job WHERE id = $1', [jobId]);
    final = rows[0]!;
    if (final.status === 'completed' || final.status === 'failed') break;
    if (Date.now() - t0 > 20 * 60_000) break;
    await new Promise((r) => setTimeout(r, 250));
  }
  const endToEndMs = Date.now() - t0;
  line('job status', final.status);
  line(
    'processed / failed',
    `${final.processed_count.toLocaleString()} / ${final.failed_count.toLocaleString()}`,
  );
  line('END-TO-END after submission', ms(endToEndMs));
  line('END-TO-END from submit', ms(endToEndMs + submitted.ms));
  line(
    'throughput',
    `${Math.round(final.processed_count / (endToEndMs / 1000)).toLocaleString()} opportunities/sec`,
  );

  const { rows: batches } = await pool.query<{
    batch_no: number;
    first_ms: number;
    last_ms: number;
    n: number;
  }>(
    `SELECT batch_no,
            min(EXTRACT(EPOCH FROM (i.completed_at - j.created_at)) * 1000)::bigint AS first_ms,
            max(EXTRACT(EPOCH FROM (i.completed_at - j.created_at)) * 1000)::bigint AS last_ms,
            count(*)::int AS n
       FROM bulk_job_item i JOIN bulk_job j ON j.id = i.job_id
      WHERE i.job_id = $1 AND i.completed_at IS NOT NULL
      GROUP BY batch_no ORDER BY batch_no`,
    [jobId],
  );
  if (batches.length > 0) {
    const d = distribution(batches.map((b) => Number(b.last_ms) - Number(b.first_ms)));
    console.log(`\nPER-BATCH processing time (${d.count} batches of ${batches[0]!.n})`);
    line('p50', ms(d.p50));
    line('p95', ms(d.p95));
    line('p99', ms(d.p99));
    line('min / max', `${ms(d.min)} / ${ms(d.max)}`);
  }

  const { rows: perItem } = await pool.query<{ ms: number }>(
    `SELECT (EXTRACT(EPOCH FROM (i.completed_at - j.created_at)) * 1000)::bigint AS ms
       FROM bulk_job_item i JOIN bulk_job j ON j.id = i.job_id
      WHERE i.job_id = $1 AND i.completed_at IS NOT NULL`,
    [jobId],
  );
  const items = distribution(perItem.map((r) => Number(r.ms)));
  console.log(
    `\nPER-OPPORTUNITY latency, submit to moved (${items.count.toLocaleString()} samples)`,
  );
  line('p50', ms(items.p50));
  line('p95', ms(items.p95));
  line('p99', ms(items.p99));
  line('min / max', `${ms(items.min)} / ${ms(items.max)}`);
  line('mean', ms(items.mean));

  console.log('\nQUERY PLAN for the worker claim (the partial index)');
  console.log(
    execSync(
      `docker exec bsm-postgres psql -U app -d bulk_stage_move -tAc "explain (analyze, costs off) update bulk_job_item set status='running' where job_id='${jobId}' and batch_no=0 and workspace_id='${ws.workspaceId}' and status in ('pending','running')"`,
    )
      .toString()
      .trim()
      .split('\n')
      .map((l) => `  ${l}`)
      .join('\n'),
  );

  await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
  await pool.end();
}

void main();
