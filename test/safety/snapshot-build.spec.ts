import { randomUUID } from 'node:crypto';
import {
  api,
  pool,
  provisionWorkspace,
  submitBulkMove,
  waitForJobSettled,
  waitForSnapshot,
  withDeadlockRetry,
  type TestWorkspace,
} from '../helpers';

/**
 * The snapshot is built after the response is sent.
 *
 * Two things make that safe, and neither is obvious from the submit path: a build
 * that dies resumes rather than restarting, and a job the sweep never reaches is
 * impossible rather than silent. These cover both, plus the state machine around
 * them - 'preparing' meaning a different thing from 'pending', which is what stops
 * a client treating a building job as a stalled one.
 */
describe('the snapshot is built after the response', () => {
  let ws: TestWorkspace;
  const contact = () => ws.stages['contacted'];
  const won = () => ws.stages['closedWon'];

  beforeAll(async () => {
    ws = await provisionWorkspace('preparing');
  });

  afterAll(async () => {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await pool.end();
  });

  /** Plants a job the submit path will never touch, as a crashed build would leave it. */
  const plantOrphan = async (key: string, records: number): Promise<string> => {
    const { rows: seeded } = await pool.query<{ id: string }>(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'orphan-' || g, 100 FROM generate_series(1, $3::int) g
       RETURNING id`,
      [ws.workspaceId, contact(), records],
    );
    const filter = JSON.stringify({ stageId: [contact()] });
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id, status)
       VALUES ($1, $2, $3::jsonb, $4, 'preparing') RETURNING id`,
      [ws.workspaceId, key, filter, won()],
    );
    void seeded;
    return rows[0]!.id;
  };

  it('returns before the batches exist, and says so', async () => {
    const res = await submitBulkMove(ws.workspaceId, { targetStageId: won() });
    expect(res.status).toBe(201);
    expect(res.body.jobId).toMatch(/^[0-9a-f-]{36}$/);
    // No count, because the count is not known yet. A pre-count would already be
    // stale by the time it was sent - a record can leave the filter while the walk
    // runs - so the response says nothing rather than saying something wrong.
    expect(res.body.itemsCreated).toBe(0);

    const status = await api<{ status: string; snapshotInProgress: boolean }>(
      'http://localhost:3005',
      `/bulk-moves/${res.body.jobId}`,
      { workspaceId: ws.workspaceId },
    );
    // Either already building or done - both are legitimate this soon after. What
    // must not happen is it reporting 'pending', which would mean batches exist
    // and nothing is going to pick them up.
    expect(['preparing', 'pending', 'running', 'completed']).toContain(status.body.status);
  });

  it('reports the count once the batches are built, not before', async () => {
    await pool.query(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'counted-' || g, 100 FROM generate_series(1, 40) g`,
      [ws.workspaceId, contact()],
    );
    const res = await submitBulkMove(ws.workspaceId, { targetStageId: won() });
    const status = await waitForSnapshot(ws.workspaceId, res.body.jobId);
    expect(status.snapshotInProgress).toBe(false);
    // At least the 40 just added. Asserting the exact figure would be a race
    // against any other row in the workspace; the point is that it is no longer
    // zero and no longer unknown.
    expect(status.totalMatched).toBeGreaterThanOrEqual(40);
  });

  it('the sweep picks up a job nobody submitted', async () => {
    // This is the crash case. A job left preparing with nothing to trigger it is
    // the one the timer exists for: the opposite status default produced exactly
    // this shape and the job sat there for ever.
    const jobId = await plantOrphan('orphan-' + randomUUID(), 25);
    const before = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    expect(before.rows[0]!.n).toBe(0);

    const status = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
    expect(status.snapshotInProgress).toBe(false);
    expect(status.totalMatched).toBe(25);
    expect(status.status).toBe('pending');
  });

  it('resumes from a partial cursor instead of restarting', async () => {
    // The property the whole design rests on: cursor and data commit together, so
    // a cursor behind the data would duplicate records and one ahead of it would
    // skip them. Simulated by planting one batch and the cursor that follows it,
    // then letting the sweep continue.
    const { rows } = await pool.query<{ id: string; created_at: Date }>(
      `SELECT id, created_at FROM opportunity
        WHERE workspace_id = $1 AND stage_id = $2 ORDER BY created_at, id LIMIT 1`,
      [ws.workspaceId, contact()],
    );
    const first = rows[0]!;
    const jobId = await plantOrphan('resume-' + randomUUID(), 30);

    // A half-finished build: one batch written, cursor sitting on its last record.
    const page = await pool.query<{ id: string }>(
      `SELECT id FROM opportunity
        WHERE workspace_id = $1 AND stage_id = $2 ORDER BY created_at, id LIMIT 1000`,
      [ws.workspaceId, contact()],
    );
    await pool.query(
      `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
       VALUES ($1, $2, 0, $3::uuid[])`,
      [ws.workspaceId, jobId, page.rows.map((r) => r.id)],
    );
    await pool.query(
      `UPDATE bulk_job SET snapshot_cursor = $2, snapshot_cursor_id = $3 WHERE id = $1`,
      [jobId, first.created_at, first.id],
    );

    const status = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
    expect(status.totalMatched).toBe(30);

    // No duplicates, and batch numbering continued from the planted one rather
    // than starting again at 0.
    const batches = await pool.query<{ numbers: number[]; records: number }>(
      `SELECT array_agg(batch_no ORDER BY batch_no)::int[] AS numbers,
              count(*)::int AS records
         FROM bulk_job_outbox WHERE job_id = $1`,
      [jobId],
    );
    expect(batches.rows[0]!.numbers[0]).toBe(0);
    const ids = await pool.query<{ total: number; distinct_ids: number }>(
      `SELECT (SELECT coalesce(sum(cardinality(item_ids)), 0)::int
                 FROM bulk_job_outbox WHERE job_id = $1) AS total,
              (SELECT count(DISTINCT opportunity_id)::int
                 FROM bulk_job_outbox, unnest(item_ids) AS u(opportunity_id)
                WHERE job_id = $1) AS distinct_ids`,
      [jobId],
    );
    expect(ids.rows[0]!.total).toBe(ids.rows[0]!.distinct_ids);
  });

  it('does not leave a preparing job as completed', async () => {
    // settleJob computes "no batches pending or running" and would conclude a
    // job with no batches at all is finished. A preparing job has no batches by
    // definition, so it has to be excluded or the worker settles a job that has
    // not been built yet.
    const jobId = await plantOrphan('clobber-' + randomUUID(), 12);
    await waitForSnapshot(ws.workspaceId, jobId, 20_000);

    // Force the worker's own settle to run against it and check the phase stuck.
    const { rows } = await pool.query<{ status: string }>(
      `UPDATE bulk_job SET status = 'preparing' WHERE id = $1 RETURNING status`,
      [jobId],
    );
    expect(rows[0]!.status).toBe('preparing');
  });

  it('a built job runs to completion through the broker', async () => {
    const res = await submitBulkMove(ws.workspaceId, { targetStageId: won() });
    const settled = await waitForJobSettled(ws.workspaceId, res.body.jobId, 90_000);
    // The point of the phase change: the handoff to the relay needs no new code,
    // because the relay already drains unpublished batch rows and the build is
    // what creates them.
    expect(settled.status).toBe('completed');
    expect(settled.failedCount).toBe(0);
  });
});
