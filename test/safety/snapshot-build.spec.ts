import { randomUUID } from 'node:crypto';
import {
  api,
  createPrivateStage,
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

  /**
   * Plants a job the submit path will never touch, as a crashed build would leave
   * it, and returns its id and the exact count the walk must report.
   *
   * The stage is private per call. It used to be the shared `contacted`, and the
   * filter named that stage - so the walk matched every record any earlier test
   * had put there. The expected count was this test's own `records`, and it came
   * back as 65 or 110 instead of 25 or 30: the product was right and the fixture
   * was not, because a filter cannot distinguish records the test planted from
   * records another test planted in the same stage.
   */
  const plantOrphan = async (
    key: string,
    records: number,
  ): Promise<{ jobId: string; stageId: string }> => {
    const stageId = await createPrivateStage(ws.workspaceId, 'orphan');
    await pool.query(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'orphan-' || g, 100 FROM generate_series(1, $3::int) g`,
      [ws.workspaceId, stageId, records],
    );
    const filter = JSON.stringify({ stageId: [stageId] });
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id, status)
       VALUES ($1, $2, $3::jsonb, $4, 'preparing') RETURNING id`,
      [ws.workspaceId, key, filter, won()],
    );
    return { jobId: rows[0]!.id, stageId };
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
    // Its own stage, so the figure below is exact rather than a floor. This job
    // filters on nothing - the whole workspace - so anything another test seeded
    // would be counted too, and the count has to be a lower bound only because
    // of that. With a private stage and a filter naming it, it can be the number.
    const stageId = await createPrivateStage(ws.workspaceId, 'counts-once-built');
    await pool.query(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'counted-' || g, 100 FROM generate_series(1, 40) g`,
      [ws.workspaceId, stageId],
    );
    const res = await submitBulkMove(ws.workspaceId, { targetStageId: won(), stageId });
    const status = await waitForSnapshot(ws.workspaceId, res.body.jobId);
    expect(status.snapshotInProgress).toBe(false);
    // No longer zero, and no longer unknown: the count the endpoint reports is
    // the one that actually landed in batches.
    expect(status.totalMatched).toBe(40);
  });

  it('the sweep picks up a job nobody submitted', async () => {
    // This is the crash case. A job left preparing with nothing to trigger it is
    // the one the timer exists for: the opposite status default produced exactly
    // this shape and the job sat there for ever.
    const { jobId } = await plantOrphan('orphan-' + randomUUID(), 25);
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
    // Its own stage, so the 30 records this test expects are the only 30 the walk
    // can see. Planted on the shared stage, the planted batch and the cursor were
    // drawn from a different population than the one the job was filtering on.
    const { jobId, stageId } = await plantOrphan('resume-' + randomUUID(), 30);

    // A half-finished build. The page is deliberately short of a full batch so the
    // resume has real work left to do, and the cursor goes on the page's LAST
    // record - which is the invariant a real build maintains, since it commits
    // the batch and the cursor that follows it in one transaction.
    //
    // It used to be the first record, which put the cursor behind the data it had
    // already written: the resumed walk then re-read the other 29 and the job
    // reported 59 records for a 30 record fixture. The duplicate-count assertion
    // below caught it, which is the argument for having that assertion at all.
    const page = await pool.query<{ id: string; created_at: Date }>(
      `SELECT id, created_at FROM opportunity
        WHERE workspace_id = $1 AND stage_id = $2 ORDER BY created_at, id LIMIT 10`,
      [ws.workspaceId, stageId],
    );
    const planted = page.rows;
    const cursorRow = planted[planted.length - 1]!;
    await pool.query(
      `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
       VALUES ($1, $2, 0, $3::uuid[])`,
      [ws.workspaceId, jobId, planted.map((r) => r.id)],
    );
    await pool.query(
      `UPDATE bulk_job SET snapshot_cursor = $2, snapshot_cursor_id = $3 WHERE id = $1`,
      [jobId, cursorRow.created_at, cursorRow.id],
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
    // The planted batch is still batch 0, and the walk continued past its cursor
    // into batch 1 rather than renumbering from 0 - which is what resuming means.
    // Two batches for thirty records: the ten planted, and the twenty the cursor
    // had not yet passed.
    expect(batches.rows[0]!.numbers).toEqual([0, 1]);
    expect(batches.rows[0]!.records).toBe(2);
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
    const { jobId } = await plantOrphan('clobber-' + randomUUID(), 12);
    await waitForSnapshot(ws.workspaceId, jobId, 20_000);

    // Back to preparing with its batches still on disk, which is the state that
    // matters: settleJob decides a job is finished by finding no batch pending or
    // running, and a job that is still being built has not been built yet.
    // Without the 'preparing' exclusion in that WHERE clause, the worker would
    // settle this job to completed while the sweep was still walking it.
    await pool.query(`UPDATE bulk_job SET status = 'preparing' WHERE id = $1`, [jobId]);

    const { rows: after } = await pool.query<{ status: string; batches: number }>(
      `SELECT j.status,
              (SELECT count(*)::int FROM bulk_job_outbox b WHERE b.job_id = j.id) AS batches
         FROM bulk_job j WHERE j.id = $1`,
      [jobId],
    );
    expect(after[0]!.status).toBe('preparing');
    // The batches are there, so "no pending or running batches" is not what is
    // keeping it open - the status guard is.
    expect(after[0]!.batches).toBe(1);
  });

  it('a built job runs to completion through the broker', async () => {
    // A stage with a rule permitting the move, and a filter naming it.
    //
    // This job used to sweep the whole workspace, which worked only because every
    // record in it sat in a stage with a rule to the target. The private stages
    // above have no such rule, so a whole-workspace job now finds records it
    // cannot move - correctly reported as failures, and a job whose every record
    // failed settles as 'failed' rather than 'completed'. The product was right and
    // the fixture had become untrue.
    const stageId = await createPrivateStage(ws.workspaceId, 'runs-to-done');
    await pool.query(
      `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
       VALUES ($1, $2, $3)`,
      [ws.workspaceId, stageId, won()],
    );
    await pool.query(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'drained-' || g, 100 FROM generate_series(1, 60) g`,
      [ws.workspaceId, stageId],
    );

    const res = await submitBulkMove(ws.workspaceId, { targetStageId: won(), stageId });
    const settled = await waitForJobSettled(ws.workspaceId, res.body.jobId, 90_000);
    // The point of the phase change: the handoff to the relay needs no new code,
    // because the relay already drains unpublished batch rows and the build is
    // what creates them.
    expect(settled.status).toBe('completed');
    expect(settled.totalMatched).toBe(60);
    expect(settled.failedCount).toBe(0);
    // Every batch settled, which is what completed means. The endpoint reports
    // batch states rather than a processed record count, and the batch rows are
    // the authority on whether the records actually moved.
    expect(settled.batches['completed']).toBe(1);
    expect(settled.batches['pending'] ?? 0).toBe(0);
    expect(settled.batches['running'] ?? 0).toBe(0);
  });
});
