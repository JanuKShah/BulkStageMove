/**
 * Interactive read latency while a 50,000-record bulk job runs.
 *
 * The question this answers is the noisy-neighbour one, and it needs two
 * workspaces, not one: a read in the workspace the job is moving is a different
 * question from a read in a workspace it is not touching. Reporting only the
 * first would let a system pass by being uniformly slow.
 *
 * Both phases are measured on both workspaces, so the degradation is a ratio
 * rather than an absolute. An absolute p95 says nothing about whether the job
 * caused it.
 *
 *   npm run bench:interactive
 */
import { request } from 'node:http';
import { pool } from '../helpers';
import { createHappyFlowWorkspace, percentile } from './fixture';

const BULK = { host: 'localhost', port: 3005 };
const READ = { host: 'localhost', port: 3004 };
const SIZE = Number(process.env.BENCH_SIZE ?? 50_000);
const SAMPLE_MS = Number(process.env.SAMPLE_MS ?? 200);
const BASELINE_MS = Number(process.env.BASELINE_MS ?? 15_000);

/** A cheap list read, which is what a user waiting on a page actually issues. */
function read(workspaceId: string): Promise<{ status: number; ms: number }> {
  return new Promise((resolve, reject) => {
    const started = process.hrtime.bigint();
    const req = request(
      {
        ...READ,
        path: '/opportunities?limit=20',
        method: 'GET',
        // undici applies its own headers timeout, which turns "the server is slow
        // under load" into a client-side abort and loses the sample entirely.
        timeout: 0,
        headers: { 'x-workspace-id': workspaceId },
      },
      (res) => {
        res.resume();
        res.on('end', () =>
          resolve({
            status: res.statusCode ?? 0,
            ms: Number(process.hrtime.bigint() - started) / 1e6,
          }),
        );
      },
    );
    req.on('error', reject);
    req.end();
  });
}

interface Series {
  label: string;
  samples: number[];
  errors: number;
}

/**
 * The loops have to be stoppable. An unbounded `for(;;)` keeps the event loop
 * alive after main() returns, so the process prints its results and then hangs
 * for ever instead of exiting — which looks exactly like a slow run.
 */
let stopped = false;

function startSampler(label: string, workspaceId: string): Series {
  const series: Series = { label, samples: [], errors: 0 };
  const loop = async (): Promise<void> => {
    while (!stopped) {
      try {
        const r = await read(workspaceId);
        if (r.status === 200) series.samples.push(r.ms);
        else series.errors++;
      } catch {
        series.errors++;
      }
      await new Promise((r) => setTimeout(r, SAMPLE_MS));
    }
  };
  void loop();
  return series;
}

function report(series: Series, during: boolean): void {
  const s = [...series.samples].sort((a, b) => a - b);
  if (s.length === 0) {
    console.log(`  ${series.label.padEnd(22)} no samples`);
    return;
  }
  const at = (p: number): string => percentile(s, p).toFixed(1).padStart(7);
  console.log(
    `  ${(during ? 'during  ' : 'quiet   ') + series.label.padEnd(22)}` +
      `n=${String(s.length).padStart(4)}` +
      `  p50${at(50)}  p95${at(95)}  p99${at(99)}  max${s[s.length - 1]!.toFixed(1).padStart(7)}` +
      (series.errors ? `  errors ${series.errors}` : ''),
  );
}

/** p99 over a handful of samples is not a percentile, so the window has to be long. */
function warnIfThin(samples: number[], elapsedMs: number): void {
  if (samples.length < 30) {
    console.log(
      `\n  WARNING: only ${samples.length} samples over ${(elapsedMs / 1000).toFixed(1)}s.` +
        `\n  p99 over that few samples is not a percentile. Re-run with a larger` +
        `\n  BENCH_SIZE so the job runs long enough to characterise.`,
    );
  }
}

async function submit(workspaceId: string, to: string, size: number): Promise<string> {
  const body = JSON.stringify({
    idempotencyKey: `interactive-${Date.now()}`,
    targetStageId: to,
    maxValue: size,
  });
  return new Promise((resolve, reject) => {
    const req = request(
      {
        ...BULK,
        path: '/bulk-moves',
        method: 'POST',
        timeout: 0,
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
          'x-workspace-id': workspaceId,
        },
      },
      (res) => {
        let out = '';
        res.on('data', (c) => (out += c));
        res.on('end', () => {
          if (res.statusCode !== 201) return reject(new Error(`submit ${res.statusCode}: ${out}`));
          resolve((JSON.parse(out) as { jobId: string }).jobId);
        });
      },
    );
    req.on('error', reject);
    req.end(body);
  });
}

async function settled(workspaceId: string, jobId: string): Promise<boolean> {
  const { rows } = await pool.query<{ status: string; processed_count: number }>(
    'SELECT status, processed_count FROM bulk_job WHERE id = $1',
    [jobId],
  );
  const j = rows[0];
  return !!j && ['completed', 'failed'].includes(j.status);
}

async function main(): Promise<void> {
  console.log(`\n  INTERACTIVE READS DURING A ${SIZE.toLocaleString('en-US')} RECORD BULK JOB`);
  console.log(`  sampling every ${SAMPLE_MS} ms, baseline ${BASELINE_MS / 1000}s\n`);

  const target = await createHappyFlowWorkspace('interactive-target', SIZE);
  const other = await createHappyFlowWorkspace('interactive-other', 5_000);
  console.log(
    `  target workspace ${target.workspaceId}  (${SIZE.toLocaleString('en-US')} records)`,
  );
  console.log(`  other  workspace ${other.workspaceId}  (5,000 records)\n`);

  const a = startSampler('same workspace', target.workspaceId);
  const b = startSampler('different workspace', other.workspaceId);

  await new Promise((r) => setTimeout(r, BASELINE_MS));

  // Everything collected before this instant is the quiet baseline; everything
  // after is "during the job". Splitting on the index rather than reporting the
  // cumulative series is the whole measurement — comparing a running series to
  // itself would report 1.0x and mean nothing.
  const split = { a: a.samples.length, b: b.samples.length };
  const jobId = await submit(target.workspaceId, target.to, SIZE);
  const started = Date.now();
  console.log(`  submitted ${jobId}; sampling until it settles\n`);

  while (!(await settled(target.workspaceId, jobId))) {
    await new Promise((r) => setTimeout(r, 250));
  }
  const elapsed = Date.now() - started;

  const quietA = a.samples.slice(0, split.a);
  const duringA = a.samples.slice(split.a);
  const quietB = b.samples.slice(0, split.b);
  const duringB = b.samples.slice(split.b);

  const show = (label: string, samples: number[], errors: number): void => {
    const s = [...samples].sort((a, b) => a - b);
    if (s.length === 0) {
      console.log(`  ${label.padEnd(24)} no samples`);
      return;
    }
    const at = (p: number): string => percentile(s, p).toFixed(1).padStart(7);
    console.log(
      `  ${label.padEnd(24)}n=${String(s.length).padStart(4)}` +
        `  p50${at(50)}  p95${at(95)}  p99${at(99)}  max${s[s.length - 1]!.toFixed(1).padStart(7)}` +
        (errors ? `  errors ${errors}` : ''),
    );
  };

  console.log('  QUIET');
  show('same workspace', quietA, a.errors);
  show('different workspace', quietB, b.errors);
  console.log();
  console.log(`  DURING THE BULK RUN  (job settled in ${(elapsed / 1000).toFixed(2)}s)`);
  show('same workspace', duringA, a.errors);
  show('different workspace', duringB, b.errors);

  const p95 = (s: number[]): number =>
    percentile(
      [...s].sort((x, y) => x - y),
      95,
    );
  console.log(
    `\n  p95 degraded by ${(p95(duringA) / p95(quietA)).toFixed(2)}x in the job's own workspace`,
  );
  console.log(
    `  p95 degraded by ${(p95(duringB) / p95(quietB)).toFixed(2)}x in a different workspace`,
  );
  warnIfThin(duringA, elapsed);
  warnIfThin(duringB, elapsed);

  // Stop the samplers before closing the pool, or their in-flight requests land
  // on a closed pool and the loop retries for ever.
  stopped = true;
  await new Promise((r) => setTimeout(r, SAMPLE_MS * 2));
  await pool.query('DELETE FROM workspace WHERE id = ANY($1)', [
    [target.workspaceId, other.workspaceId],
  ]);
  await pool.end();
}

void main();
