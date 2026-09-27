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
  console.log(`building a ${HAPPY_FLOW_SIZE.toLocaleString('en-US')} opportunity fixture...`);
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
  line('items created', created.itemsCreated.toLocaleString('en-US'));
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
    `${final.processed_count.toLocaleString('en-US')} / ${final.failed_count.toLocaleString('en-US')}`,
  );
  line('END-TO-END after submission', ms(endToEndMs));
  line('END-TO-END from submit', ms(endToEndMs + submitted.ms));
  line(
    'throughput',
    `${Math.round(final.processed_count / (endToEndMs / 1000)).toLocaleString('en-US')} opportunities/sec`,
  );

  // Per-batch completion, read off the batch rows. A batch has one completed_at
  // for its whole page, so this is the time the batch finished rather than a
  // distribution over records - the previous per-item form reported 0ms for every
  // intra-batch percentile because all the records shared one timestamp.
  const { rows: batches } = await pool.query<{
    batch_no: number;
    first_ms: number;
    last_ms: number;
    n: number;
  }>(
    `SELECT batch_no,
            EXTRACT(EPOCH FROM (b.completed_at - j.created_at)) * 1000 AS last_ms,
            EXTRACT(EPOCH FROM (b.started_at - j.created_at)) * 1000 AS first_ms,
            cardinality(b.item_ids)::int AS n
       FROM bulk_job_outbox b JOIN bulk_job j ON j.id = b.job_id
      WHERE b.job_id = $1 AND b.completed_at IS NOT NULL
      ORDER BY batch_no`,
    [jobId],
  );
  if (batches.length > 0) {
    console.log(`\nPER-BATCH completion, submit to batch finished (${batches.length} batches)`);
    const d = distribution(batches.map((b) => Number(b.last_ms)));
    line('p50', ms(d.p50));
    line('p95', ms(d.p95));
    line('p99', ms(d.p99));
    line('min / max', `${ms(d.min)} / ${ms(d.max)}`);
  }

  // Per-opportunity latency, from the transitions the job actually caused. That
  // is the only per-record evidence left: there is no item table, so a record's
  // "moved at" is the transition it produced.
  // Per-opportunity latency, from the transitions the job caused.
  //
  // These are NOT 50,000 independent samples. A batch applies in one
  // transaction, so all 1,000 of its transitions are inserted together and share
  // one created_at - the distinct-timestamp count below is the proof, and it is
  // the number of batches, not the number of records. Reporting p95 over the
  // expanded set would imply a tail that does not exist: every percentile here
  // resolves to the same 50 batch times.
  //
  // So this is printed as what it is - the batch distribution, restated - rather
  // than as a per-record percentile it cannot support.
  const { rows: perItem } = await pool.query<{ n: number; distinct_ts: number }>(
    `SELECT count(*)::int AS n,
            count(DISTINCT created_at)::int AS distinct_ts
       FROM opportunity_transition
      WHERE job_id = $1`,
    [jobId],
  );
  if (perItem.length > 0) {
    console.log(
      `\nPER-OPPORTUNITY latency, submit to moved (${perItem[0]!.n.toLocaleString('en-US')} records)`,
    );
    line('distinct timestamps', perItem[0]!.distinct_ts.toLocaleString('en-US'));
    console.log(
      '  one per batch: a batch applies in one transaction, so its 1,000 records\n' +
        '  share a created_at. Percentiles over the expanded set are the batch\n' +
        '  distribution above, not a per-record tail.',
    );
  }

  console.log('\nQUERY PLAN for the worker claim (the partial index)');
  console.log(
    execSync(
      `docker exec bsm-postgres psql -U app -d bulk_stage_move -tAc "explain (analyze, costs off) update bulk_job_outbox set status='running' where job_id='${jobId}' and batch_no=0 and workspace_id='${ws.workspaceId}' and status in ('pending','running')"`,
    )
      .toString()
      .trim()
      .split('\n')
      .map((l) => `  ${l}`)
      .join('\n'),
  );

  // Where the wall clock went, per phase.
  //
  // The four timestamps on a batch row - written, published, claimed, settled -
  // are the whole pipeline except one thing, and the exception matters.
  //
  // now() in Postgres is the *transaction start* timestamp, not the statement
  // time. started_at is written in the claim transaction and completed_at in the
  // work transaction, so completed_at - started_at is the gap between the two
  // BEGINs - a commit plus a round trip - and not the apply. The apply happens
  // inside the work transaction, after now() has already been captured, so its
  // cost appears nowhere in the database. Measured on this run that gap is 17ms
  // against a real apply time of 529ms.
  //
  // So apply time has to come from the worker log, which times processBatch
  // in-process. That is what the BatchWorker `work=` field is for, and it is the
  // only place the number exists. Everything else here is sound: created_at,
  // published_at and started_at are each the start of a different transaction,
  // so the deltas between them are real elapsed time.
  //
  // The sum column is the point for the sequential phases. Whatever the phases
  // do not account for is time no process was doing anything, which is the only
  // genuinely recoverable time in the run.
  const { rows: phases } = await pool.query<{
    phase: string;
    min_ms: number | null;
    avg_ms: number | null;
    max_ms: number | null;
    sum_ms: number | null;
  }>(
    `WITH b AS (
       SELECT o.batch_no,
              o.created_at, o.published_at, o.started_at, o.completed_at,
              j.created_at AS job_created, j.completed_at AS job_done,
              EXTRACT(EPOCH FROM (o.created_at
                - LAG(o.created_at, 1, j.created_at) OVER (ORDER BY o.batch_no))) * 1000
                AS build_ms,
              EXTRACT(EPOCH FROM (o.published_at - o.created_at)) * 1000 AS relay_ms,
              EXTRACT(EPOCH FROM (o.started_at  - o.published_at)) * 1000 AS queue_ms,
              EXTRACT(EPOCH FROM (o.completed_at - o.started_at))  * 1000 AS work_ms
         FROM bulk_job_outbox o
         JOIN bulk_job j ON j.id = o.job_id
        WHERE o.job_id = $1 AND o.completed_at IS NOT NULL
     )
     SELECT 'whole job   submit -> done' AS phase,
            round(min(EXTRACT(EPOCH FROM (job_done - job_created)) * 1000))::bigint AS min_ms,
            round(avg(EXTRACT(EPOCH FROM (job_done - job_created)) * 1000))::bigint AS avg_ms,
            round(max(EXTRACT(EPOCH FROM (job_done - job_created)) * 1000))::bigint AS max_ms,
            NULL::bigint AS sum_ms
       FROM b
     UNION ALL
     SELECT 'sweep wait  submit -> page 0',
            round(EXTRACT(EPOCH FROM (min(created_at) - min(job_created))) * 1000)::bigint,
            NULL, NULL, NULL FROM b
     UNION ALL
     SELECT 'build       per page', round(min(build_ms))::bigint, round(avg(build_ms))::bigint,
            round(max(build_ms))::bigint, round(sum(build_ms))::bigint FROM b
     UNION ALL
     SELECT 'relay lag   written -> published', round(min(relay_ms))::bigint,
            round(avg(relay_ms))::bigint, round(max(relay_ms))::bigint,
            round(sum(relay_ms))::bigint FROM b
     UNION ALL
     SELECT 'queue wait  in rabbitmq', round(min(queue_ms))::bigint, round(avg(queue_ms))::bigint,
            round(max(queue_ms))::bigint, round(sum(queue_ms))::bigint FROM b
     UNION ALL
     SELECT 'claim gap   claim txn -> work txn', round(min(work_ms))::bigint, round(avg(work_ms))::bigint,
            round(max(work_ms))::bigint, round(sum(work_ms))::bigint FROM b
     UNION ALL
     SELECT 'tail        last batch -> done', NULL, NULL, NULL,
            round(EXTRACT(EPOCH FROM (max(job_done) - max(completed_at))) * 1000)::bigint
       FROM b`,
    [jobId],
  );

  console.log('\nWHERE THE TIME WENT  (min / mean / max, and total across all batches)');
  for (const p of phases) {
    const triple = [p.min_ms, p.avg_ms, p.max_ms].map((v) => (v === null ? '     ' : ms(v)));
    const total = p.sum_ms === null ? '' : `   sum ${ms(p.sum_ms)}`;
    line(p.phase, `${triple.join(' / ')}${total}`);
  }

  // Reconciliation: the phases are sequential within a batch, so their totals
  // should add up to something close to the whole job. The gap is idle time.
  const total = phases.find((p) => p.phase.startsWith('whole job'))?.min_ms ?? 0;
  const accounted = phases
    .filter((p) => p.sum_ms !== null && p.phase.startsWith('build'))
    .reduce((a, p) => a + (p.sum_ms ?? 0), 0);
  const workTotal =
    phases.find((p) => p.phase.startsWith('work'))?.sum_ms ?? 0;
  console.log(
    `\n  build total ${ms(accounted)} and work total ${ms(workTotal)} across ` +
      `${batches.length} batches. Twelve slots, so the work total is wall-clock ` +
      `parallelism, not elapsed time - compare it against the whole job rather ` +
      `than adding to it.`,
  );
  line('work total / whole job', `${Math.round((workTotal / Math.max(total, 1)) * 100)}%`);

  await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
  await pool.end();
}

void main();
