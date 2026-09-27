/**
 * Per-batch timing for a completed 50,000 record bulk job.
 *
 * The subtlety this exists to handle: every item in a batch shares one
 * completed_at, because the worker applies a batch in a single transaction. So
 * max(completed_at) - min(completed_at) within a batch is exactly 0 ms and
 * reports nothing. What is observable is each batch's completion relative to the
 * previous one, which is the gap between when batch N committed and when N-1
 * did. For the first batch that is the time from job creation to first
 * completion.
 *
 *   npx tsx test/happyflow/batch-timing.ts [jobId]
 */
import { pool } from '../helpers';
import { distribution, ms, percentile } from './fixture';

interface Row {
  batch_no: number;
  done_ms: number;
  gap_ms: number;
  n: number;
}

async function main(): Promise<void> {
  const arg = process.argv[2];
  const { rows: jobs } = await pool.query<{ id: string; total_matched: number }>(
    arg
      ? 'SELECT id, total_matched FROM bulk_job WHERE id = $1'
      : `SELECT id, total_matched FROM bulk_job
          WHERE total_matched = 50000 AND status = 'completed'
          ORDER BY created_at DESC LIMIT 1`,
  );
  const job = jobs[0];
  if (!job) {
    console.log('no completed 50,000 record job found');
    await pool.end();
    return;
  }

  const { rows } = await pool.query<{ batch_no: number; done_ms: number; n: number }>(
    `SELECT i.batch_no,
            (EXTRACT(EPOCH FROM (i.completed_at - j.created_at)) * 1000)::bigint AS done_ms,
            cardinality(i.item_ids)::int AS n
       FROM bulk_job_outbox i JOIN bulk_job j ON j.id = i.job_id
      WHERE i.job_id = $1 AND i.completed_at IS NOT NULL
      ORDER BY i.batch_no`,
    [job.id],
  );

  const batches: Row[] = rows.map((r) => ({ ...r, gap_ms: 0 }));
  for (let i = 0; i < batches.length; i++) {
    batches[i]!.gap_ms =
      i === 0 ? batches[0]!.done_ms : batches[i]!.done_ms - batches[i - 1]!.done_ms;
  }

  console.log(
    `job ${job.id}  ${job.total_matched.toLocaleString()} records in ${batches.length} batches\n`,
  );
  console.log('batch  records  completed at   gap from previous');
  for (const b of batches) {
    console.log(
      `  ${String(b.batch_no).padStart(2)}   ${String(b.n).padStart(4)}    ${ms(b.done_ms).padStart(8)}   ${ms(b.gap_ms).padStart(8)}`,
    );
  }

  const gaps = batches.map((b) => b.gap_ms).sort((a, b) => a - b);
  const done = batches.map((b) => b.done_ms).sort((a, b) => a - b);
  const d = distribution(gaps);

  console.log(`\nGAP BETWEEN CONSECUTIVE BATCHES (${gaps.length} samples)`);
  console.log(`  min     ${ms(d.min)}`);
  console.log(`  p50     ${ms(d.p50)}`);
  console.log(`  p95     ${ms(d.p95)}`);
  console.log(`  p99     ${ms(d.p99)}`);
  console.log(`  max     ${ms(d.max)}`);
  console.log(`  mean    ${ms(d.mean)}`);

  const total = done[done.length - 1]!;
  const slowest = gaps
    .slice()
    .sort((a, b) => b - a)
    .slice(0, 5);
  console.log(`\nWHOLE JOB (submit -> last batch committed)  ${ms(total)}`);
  console.log(`  records                                  ${job.total_matched.toLocaleString()}`);
  console.log(
    `  throughput                               ${Math.round(job.total_matched / (total / 1000)).toLocaleString()} per second`,
  );
  console.log(`  mean gap between batches                 ${ms(total / batches.length)}`);
  console.log(`  5 slowest gaps                           ${slowest.map((g) => ms(g)).join(', ')}`);

  const head = gaps.slice(0, Math.ceil(gaps.length / 2));
  const tail = gaps.slice(Math.ceil(gaps.length / 2));
  console.log(
    `\n  first half of the gaps  mean ${ms(head.reduce((a, b) => a + b, 0) / head.length)}`,
  );
  console.log(
    `  second half of the gaps mean ${ms(tail.reduce((a, b) => a + b, 0) / tail.length)}`,
  );

  void percentile;
  await pool.end();
}

void main();
