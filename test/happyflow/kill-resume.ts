/**
 * Kill the builder mid-walk, then prove the resumed job is correct.
 *
 * The claim under test is DESIGN.md § 1: a half-built job is finished by the
 * scheduler rather than restarted, because the cursor is committed with the batch
 * it follows. A unit test can assert the cursor is read on resume; it cannot show
 * that a real SIGKILL loses no records and duplicates none, which is the part a
 * reviewer would doubt.
 *
 * SIGKILL, not a graceful stop, so nothing gets a chance to clean up. Docker's
 * `restart: unless-stopped` brings the service back on its own, which is the
 * realistic crash and also the realistic recovery.
 *
 *   npm run bench:killresume
 */
import { execFileSync } from 'node:child_process';
import { request } from 'node:http';
import { pool } from '../helpers';
import { createHappyFlowWorkspace } from './fixture';

const BULK = { host: 'localhost', port: 3005 };
const SIZE = Number(process.env.BENCH_SIZE ?? 50_000);
/** Batches to let the walk write before pulling the plug. */
const KILL_AFTER = Number(process.env.KILL_AFTER ?? 10);

function submit(workspaceId: string, to: string, size: number): Promise<string> {
  const body = JSON.stringify({
    idempotencyKey: `killresume-${Date.now()}`,
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

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Every wait below is bounded. A harness that polls `for (;;)` hangs forever the
 * moment the thing it is waiting for never happens, and a hung benchmark is
 * indistinguishable from a slow one until someone notices the container is idle.
 */
const TIMEOUT_MS = Number(process.env.HARNESS_TIMEOUT_MS ?? 300_000);

async function until(what: string, check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + TIMEOUT_MS;
  for (;;) {
    if (await check()) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out after ${TIMEOUT_MS}ms waiting for ${what}`);
    }
    await sleep(100);
  }
}

/**
 * execFileSync, not execSync: an argv array needs no shell, so nothing in a
 * service name can be reinterpreted as syntax. This command kills a container,
 * which is the last place to want quoting surprises.
 */
function docker(...args: string[]): string {
  return execFileSync('docker', ['compose', ...args], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

async function batchesWritten(jobId: string): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
    [jobId],
  );
  return rows[0]?.n ?? 0;
}

async function jobRow(
  jobId: string,
): Promise<{ status: string; processed_count: number; failed_count: number } | undefined> {
  const { rows } = await pool.query<{
    status: string;
    processed_count: number;
    failed_count: number;
  }>('SELECT status, processed_count, failed_count FROM bulk_job WHERE id = $1', [jobId]);
  return rows[0];
}

async function main(): Promise<void> {
  console.log(`\n  KILL THE BUILDER MID-WALK, THEN PROVE THE RESULT`);
  console.log(`  ${SIZE.toLocaleString('en-US')} records, SIGKILL after ${KILL_AFTER} batches\n`);

  const ws = await createHappyFlowWorkspace('killresume', SIZE);
  console.log(`  workspace ${ws.workspaceId}`);

  const jobId = await submit(ws.workspaceId, ws.to, SIZE);
  console.log(`  submitted ${jobId}`);

  // Wait until the walk is genuinely mid-flight, not still on page one.
  await until(
    `${KILL_AFTER} batches written`,
    async () => (await batchesWritten(jobId)) >= KILL_AFTER,
  );
  console.log(`  ${await batchesWritten(jobId)} batches written; killing transition-service now\n`);

  const killedAt = Date.now();
  docker('kill', '-s', 'SIGKILL', 'transition-service');
  // `restart: unless-stopped` does NOT revive a container that was deliberately
  // stopped, and `docker compose kill` stops it deliberately — so without this
  // the service never returns and the wait below times out. An orchestrator
  // would restart the process, so the harness does too.
  docker('start', 'transition-service');
  console.log('  SIGKILL sent; service restarting');

  // Docker restarts it under `restart: unless-stopped`. Poll the API rather than
  // docker ps, so the clock stops when the service can actually serve again.
  const ready = async (): Promise<boolean> => {
    try {
      const { status } = await new Promise<{ status: number }>((resolve, reject) => {
        const req = request(
          { ...BULK, path: '/health/ready', method: 'GET', timeout: 2000 },
          (res) => {
            res.resume();
            res.on('end', () => resolve({ status: res.statusCode ?? 0 }));
          },
        );
        req.on('error', reject);
        req.end();
      });
      return status === 200;
    } catch {
      return false; // not up yet
    }
  };
  await until('transition-service to answer again', ready);
  const backAt = Date.now();
  console.log(`  service answering again after ${((backAt - killedAt) / 1000).toFixed(2)}s`);

  await until('the job to settle', async () => {
    const j = await jobRow(jobId);
    return !!j && ['completed', 'failed'].includes(j.status);
  });
  const doneAt = Date.now();
  const j = (await jobRow(jobId))!;

  console.log(
    `\n  job ${j.status}: ${j.processed_count.toLocaleString('en-US')} processed, ${j.failed_count} failed`,
  );
  console.log(`  kill -> serving   ${((backAt - killedAt) / 1000).toFixed(2)}s`);
  console.log(`  serving -> settled ${((doneAt - backAt) / 1000).toFixed(2)}s`);
  console.log(`  kill -> settled   ${((doneAt - killedAt) / 1000).toFixed(2)}s`);

  // ---- correctness, which is the point of the exercise
  console.log('\n  IS THE RESULT CORRECT?');
  const checks = await pool.query<{
    in_target: number;
    moves: number;
    distinct_moved: number;
    batch_rows: number;
    distinct_batch_no: number;
    min_batch: number;
    max_batch: number;
  }>(
    `SELECT
       (SELECT count(*)::int FROM opportunity
         WHERE workspace_id = $1 AND stage_id = $2)              AS in_target,
       (SELECT count(*)::int FROM opportunity_transition
         WHERE job_id = $3)                                      AS moves,
       (SELECT count(DISTINCT opportunity_id)::int FROM opportunity_transition
         WHERE job_id = $3)                                      AS distinct_moved,
       (SELECT count(*)::int FROM bulk_job_outbox
         WHERE job_id = $3)                                      AS batch_rows,
       (SELECT count(DISTINCT batch_no)::int FROM bulk_job_outbox
         WHERE job_id = $3)                                      AS distinct_batch_no,
       (SELECT min(batch_no) FROM bulk_job_outbox
         WHERE job_id = $3)                                      AS min_batch,
       (SELECT max(batch_no) FROM bulk_job_outbox
         WHERE job_id = $3)                                      AS max_batch`,
    [ws.workspaceId, ws.to, jobId],
  );
  const c = checks.rows[0]!;
  const excluded = ws.excluded.length;

  // A non-zero exit is the point: a harness that reports a wrong result and still
  // exits 0 is worse than no harness, because it looks like a pass.
  let allPassed = true;
  const expect = (ok: boolean, label: string, detail: string): void => {
    if (!ok) allPassed = false;
    console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${label.padEnd(34)} ${detail}`);
  };
  console.log('');
  expect(
    c.in_target === SIZE,
    'every record in the target stage',
    `${c.in_target.toLocaleString('en-US')} of ${SIZE.toLocaleString('en-US')}`,
  );
  expect(
    c.moves === SIZE,
    'exactly one transition per record',
    `${c.moves.toLocaleString('en-US')} transitions`,
  );
  expect(
    c.distinct_moved === SIZE,
    'no record moved twice',
    `${c.distinct_moved.toLocaleString('en-US')} distinct opportunities`,
  );
  expect(
    c.batch_rows === SIZE / 1000 && c.distinct_batch_no === SIZE / 1000,
    'no batch re-walked after the kill',
    `${c.batch_rows} rows, ${c.distinct_batch_no} distinct batch_no, ${c.min_batch}..${c.max_batch}`,
  );
  expect(
    c.in_target + excluded === SIZE + excluded,
    'the filter margin was left alone',
    `${c.in_target} moved + ${excluded} excluded = ${c.in_target + excluded}`,
  );
  expect(j.failed_count === 0, 'no failed records', `${j.failed_count}`);

  await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
  await pool.end();
  if (!allPassed) process.exitCode = 1;
}

void main();
