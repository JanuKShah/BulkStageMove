import { pool, waitForJobSettled, waitForSnapshot } from '../helpers';
import {
  createHappyFlowWorkspace,
  jobFilter,
  HAPPY_FLOW_SIZE,
  type HappyFlowWorkspace,
} from './fixture';
import { withDeadlockRetry } from '../helpers';

/**
 * Regression: the submit page loop must advance over rows that share a timestamp.
 *
 * Opportunities written in one transaction all get the same created_at, because
 * now() is the transaction clock. That is the normal case for any bulk import,
 * and it is what breaks a cursor built by binding created_at as a parameter: the
 * timestamptz carries microseconds, the JS Date carries milliseconds, so the
 * truncated cursor compares below every remaining row, the same page is
 * returned for ever, and every insert conflicts. The job then reports completed
 * having moved one page of the match - 1,000 of 50,000 - which is silent
 * under-application, the worst failure this system can have.
 *
 * Small enough to run in CI: 2,500 records is three pages, which is all it
 * takes to catch it.
 */
jest.setTimeout(15 * 60_000);

describe('submit pages over rows sharing a created_at', () => {
  let ws: HappyFlowWorkspace;

  beforeAll(async () => {
    ws = await createHappyFlowWorkspace('shared-ts', 2_500);
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(DISTINCT created_at)::int AS n FROM opportunity WHERE workspace_id = $1',
      [ws.workspaceId],
    );
    // The premise, asserted: if these rows did not share a timestamp the test
    // would pass for the wrong reason.
    expect(rows[0]!.n).toBe(1);
  });

  afterAll(async () => {
    // The pool is shared with the suite below, so only the last one closes it.
    //
    // Retried, because the delete cascades across opportunity, bulk_job_outbox
    // and opportunity_transition while the worker may still be settling the job
    // - and settleJob takes a row lock on bulk_job that this delete needs. The
    // two deadlock, Postgres kills one, and the suite fails after every test in it
    // has already passed. Same reason every other suite's teardown retries.
    if (ws) await withDeadlockRetry(() => pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]));
  });

  it('snapshots every matching record, not just the first page', async () => {
    const res = await fetch('http://localhost:3005/bulk-moves', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-workspace-id': ws.workspaceId },
      body: JSON.stringify({
        idempotencyKey: `shared-${Date.now()}`,
        // 2,500, not the 50,000 default: the fixture is 2,500 records plus a
        // 2,000 margin that the filter must exclude.
        ...jobFilter(ws.to, 2_500),
      }),
    });
    const body = (await res.json()) as { jobId: string };
    expect(res.status).toBe(201);
    // 2,500 of 2,500: if the cursor stalled this would be 1,000. Read from the
    // job rather than the response, because the batches are built after it.
    const snapshot = await waitForSnapshot(ws.workspaceId, body.jobId, 30_000);
    expect(snapshot.totalMatched).toBe(2_500);

    const { rows } = await pool.query<{ n: number; batches: number }>(
      `SELECT coalesce(sum(cardinality(item_ids)), 0)::int AS n, count(*)::int AS batches
         FROM bulk_job_outbox WHERE job_id = $1`,
      [body.jobId],
    );
    expect(rows[0]).toEqual({ n: 2_500, batches: 3 });
  });
});

/**
 * The full brief at full scale: one bulk move over 50,000 opportunities.
 *
 * These assert correctness at scale, not speed. A latency assertion here would
 * fail on a loaded CI box and pass on a laptop, so the timings live in
 * benchmark.ts, which is run deliberately and reports rather than asserts.
 */
describe('happy flow: 50,000 opportunities in one job', () => {
  let ws: HappyFlowWorkspace;
  let jobId: string;

  beforeAll(async () => {
    ws = await createHappyFlowWorkspace('spec');
    const res = await fetch('http://localhost:3005/bulk-moves', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-workspace-id': ws.workspaceId },
      body: JSON.stringify({ idempotencyKey: `spec-${Date.now()}`, ...jobFilter(ws.to) }),
    });
    const body = (await res.json()) as { jobId: string; itemsCreated: number };
    expect(res.status).toBe(201);
    jobId = body.jobId;

    // The response carries no count. The batches are built after it is sent, and
    // the count that matters is the one that landed in them - a record can leave
    // the filter while the walk runs, so a count taken up front would already be
    // stale. So this waits, then reads totalMatched from the job.
    const snapshot = await waitForSnapshot(ws.workspaceId, jobId, 60_000);
    expect(snapshot.totalMatched).toBe(HAPPY_FLOW_SIZE);

    await waitForJobSettled(ws.workspaceId, jobId, 180_000);
  });

  afterAll(async () => {
    if (ws) await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
    await pool.end();
  });

  it('reaches completed with nothing failed', async () => {
    const { rows } = await pool.query<{
      status: string;
      total_matched: number;
      processed_count: number;
      failed_count: number;
    }>('SELECT status, total_matched, processed_count, failed_count FROM bulk_job WHERE id = $1', [
      jobId,
    ]);
    expect(rows[0]).toMatchObject({
      status: 'completed',
      total_matched: HAPPY_FLOW_SIZE,
      processed_count: HAPPY_FLOW_SIZE,
      failed_count: 0,
    });
  });

  it('splits the work into 50 batches of 1,000', async () => {
    // Batches are the unit of dispatch and of retry, so the count is the
    // contract, not an implementation detail. A batch holds its records as ids
    // rather than one row each, so this checks the ids it holds.
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    expect(rows[0]!.n).toBe(50);

    const per = await pool.query<{ n: number }>(
      'SELECT cardinality(item_ids)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    expect(per.rows.every((r) => r.n === 1_000)).toBe(true);
    expect(per.rows.reduce((a, r) => a + r.n, 0)).toBe(HAPPY_FLOW_SIZE);
  });

  it('completes every batch and never retries one', async () => {
    const { rows } = await pool.query<{ status: string; n: number; max_attempts: number }>(
      `SELECT status, count(*)::int AS n, max(attempts)::int AS max_attempts
         FROM bulk_job_outbox WHERE job_id = $1 GROUP BY status`,
      [jobId],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: 'completed', n: 50, max_attempts: 1 });
    // Every batch settled the full thousand it was given, and none recorded a
    // failure. The job's own counters are derived from these, so this is where a
    // short batch would show up first.
    const settled = await pool.query<{ completed: number; failed: number }>(
      `SELECT sum(completed_count)::int AS completed, sum(failed_count)::int AS failed
         FROM bulk_job_outbox WHERE job_id = $1`,
      [jobId],
    );
    expect(settled.rows[0]).toEqual({ completed: HAPPY_FLOW_SIZE, failed: 0 });
  });

  it('moves all 50,000 to the target stage', async () => {
    // The transitions this job caused are the record of what it moved, and they
    // carry the ids, so this checks the count without scanning a 50,000-element
    // array per row. The stage check is done in application terms: the transition
    // row's to_stage_id is what the worker wrote, and the job's target is fixed.
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM opportunity_transition
        WHERE job_id = $1 AND to_stage_id = $2`,
      [jobId, ws.to],
    );
    expect(rows[0]!.n).toBe(HAPPY_FLOW_SIZE);
  });

  it('gives every record its own batch, with no overlap', async () => {
    // A record appearing in two batches would be moved twice and counted twice.
    // This is the property that makes paging by keyset safe, and it is the only
    // place a duplicate could hide now that membership is stored.
    //
    // Two subqueries, because joining a set-returning function to the batch rows
    // duplicates them: cardinality(item_ids) would be summed once per id it
    // contains, squaring the total.
    const { rows } = await pool.query<{ records: number; distinct_ids: number }>(
      `SELECT (SELECT coalesce(sum(cardinality(item_ids)), 0)::int
                 FROM bulk_job_outbox WHERE job_id = $1) AS records,
              (SELECT count(DISTINCT opportunity_id)::int
                 FROM bulk_job_outbox, unnest(item_ids) AS u(opportunity_id)
                WHERE job_id = $1) AS distinct_ids`,
      [jobId],
    );
    expect(rows[0]!.records).toBe(HAPPY_FLOW_SIZE);
    expect(rows[0]!.distinct_ids).toBe(HAPPY_FLOW_SIZE);
  });

  it('records one attributable transition per record', async () => {
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM opportunity_transition WHERE job_id = $1',
      [jobId],
    );
    expect(rows[0]!.n).toBe(HAPPY_FLOW_SIZE);
  });

  it('leaves the records the filter excluded alone', async () => {
    // The filter selected a value band, so the margin above it must not move.
    // Without this a bug that ignored the filter would still pass every count.
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM opportunity WHERE id = ANY($1::uuid[]) AND stage_id <> $2',
      [ws.excluded, ws.to],
    );
    expect(rows[0]!.n).toBe(ws.excluded.length);
  });

  it('publishes every batch to the broker', async () => {
    const { rows } = await pool.query<{ n: number; unpublished: number }>(
      `SELECT count(*)::int AS n,
              count(*) FILTER (WHERE published_at IS NULL)::int AS unpublished
         FROM bulk_job_outbox WHERE job_id = $1`,
      [jobId],
    );
    expect(rows[0]!.n).toBe(50);
    expect(rows[0]!.unpublished).toBe(0);
  });

  it('settles a terminal completed_at', async () => {
    const { rows } = await pool.query<{ started_at: Date | null; completed_at: Date | null }>(
      'SELECT started_at, completed_at FROM bulk_job WHERE id = $1',
      [jobId],
    );
    expect(rows[0]!.started_at).not.toBeNull();
    expect(rows[0]!.completed_at).not.toBeNull();
  });
});
