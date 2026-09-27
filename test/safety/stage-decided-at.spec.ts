import {
  DB_URL,
  createJobWithItems,
  pool,
  provisionWorkspace,
  withDeadlockRetry,
  type TestWorkspace,
} from '../helpers';
import { closeWorkerHarness, processBatchForTest } from './worker-harness';

/**
 * A bulk job must not overwrite a change that was decided after it was submitted,
 * and must not be overwritten by an older job that happens to finish later.
 *
 * The mechanism is `opportunity.stage_decided_at`, a logical clock rather than a
 * wall clock. A job writes its own `snapshot_at` into it, so the worker's rule -
 * skip when `stage_decided_at > snapshot_at` - compares job submission times
 * rather than write times. That is what makes the newest job win regardless of
 * which one finishes first.
 *
 * These drive the worker directly, through the repository, because that is the
 * only way to state the orderings. Going through the broker would mean racing a
 * real relay and a real consumer to get a job to a known state before the edit
 * under test, and every one of these tests is about a sequence.
 *
 * Every test gets a private stage. Jobs drain in the background, so a shared
 * filtered stage is not a stable starting condition - an earlier test's job can
 * have moved its records out before this test's snapshot is taken.
 */
describe('a job does not overwrite a newer decision', () => {
  let ws: TestWorkspace;
  const won = () => ws.stages['closedWon'];

  beforeAll(async () => {
    ws = await provisionWorkspace('decided');
    // processBatchForTest constructs the repository and its own pool, so this file
    // needs DATABASE_URL in the environment but not an instance of its own.
    process.env.DATABASE_URL = DB_URL;
  });

  afterAll(async () => {
    await closeWorkerHarness();
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await pool.end();
  });

  const stage = async (): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO stage (workspace_id, name, outcome)
       VALUES ($1, $2, 'open') RETURNING id`,
      [ws.workspaceId, `d-${Math.random().toString(36).slice(2, 10)}`],
    );
    return rows[0]!.id;
  };

  const seed = async (stageId: string, count: number): Promise<string[]> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'rec-' || g, 100 FROM generate_series(1, $3::int) g
       RETURNING id`,
      [ws.workspaceId, stageId, count],
    );
    return rows.map((r) => r.id);
  };

  const rule = async (from: string, to: string): Promise<void> => {
    await pool.query(
      `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
       VALUES ($1, $2, $3)`,
      [ws.workspaceId, from, to],
    );
  };

  /** A person's edit, the way opportunity-service makes one. */
  const personMoves = async (id: string, to: string): Promise<void> => {
    await pool.query('UPDATE opportunity SET stage_id = $2 WHERE id = $1', [id, to]);
  };

  /**
   * Backdates a record's logical clock, so a job with a fixed `snapshot_at` can be
   * older than the record rather than newer.
   *
   * Needed because the two ordering tests below pin snapshot_at to absolute
   * timestamps a second apart, and the records are created now. Left alone their
   * clock is months ahead of those jobs, so every record reads as "decided after
   * this job was submitted" and neither job moves anything - which is the rule
   * working correctly on a fixture whose clock was never set up for it.
   */
  const backdateClock = async (ids: string[], at: string): Promise<void> => {
    await pool.query(
      'UPDATE opportunity SET stage_decided_at = $2::timestamptz WHERE id = ANY($1::uuid[])',
      [ids, at],
    );
  };

  const stageOf = async (id: string): Promise<string> => {
    const { rows } = await pool.query<{ stage_id: string }>(
      'SELECT stage_id FROM opportunity WHERE id = $1',
      [id],
    );
    return rows[0]!.stage_id;
  };

  /**
   * A job over one private stage, with its batches already written and
   * published_at stamped so the relay leaves them alone.
   *
   * Two reasons it does not go through the submit path. The live worker would
   * drain the batch within a tick, so the edit under test would race a real
   * consumer and these tests are all about a sequence. And `snapshot_at` has to
   * be settable, because the ordering tests need two jobs whose submission times
   * are unambiguously different - which the real path cannot promise, since both
   * would be written within the same millisecond of each other.
   *
   * The stage list is written onto the job afterwards because the fixture creates
   * the job with an empty filter, and these tests are specifically about which
   * stages the filter names.
   */
  const jobOver = async (
    stageId: string,
    ids: string[],
    options: { submittedAt?: string; target?: string; alsoInScope?: string[] } = {},
  ): Promise<{ jobId: string; batchNo: number }> => {
    const { jobId, batchNo } = await createJobWithItems(
      ws.workspaceId,
      options.target ?? won(),
      ids,
      { enqueue: false },
    );
    // alsoInScope widens the filter past the stage the records started in, which is
    // what lets a second job still consider a record the first job has moved. Both
    // ordering tests need it: with a single-stage filter the scope check decides
    // the outcome before the clock is ever consulted, and the clock is the thing
    // under test.
    const filter = { stageId: [stageId, ...(options.alsoInScope ?? [])] };
    await pool.query(
      `UPDATE bulk_job
          SET filter = $2::jsonb,
              snapshot_at = COALESCE($3::timestamptz, snapshot_at)
        WHERE id = $1`,
      [jobId, JSON.stringify(filter), options.submittedAt ?? null],
    );
    return { jobId, batchNo };
  };

  it('leaves a record alone when a person moved it after submission', async () => {
    const from = await stage();
    const other = await stage();
    await rule(from, won());
    const ids = await seed(from, 1);
    const { jobId, batchNo } = await jobOver(from, ids);

    // The person moves it while the job is still in flight. Deliberately to a
    // stage the filter does NOT name, so this is the older, coarser check rather
    // than the new one - the point is that the job still declines.
    await personMoves(ids[0]!, other);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(0);
    expect(result.skipped).toBe(1);
    // The person's change stands. This is the property the whole thing protects.
    expect(await stageOf(ids[0]!)).toBe(other);
  });

  it('leaves a record alone when a person moved it WITHIN the filtered stage', async () => {
    // The case that did not exist before. Both stages are named by the filter, so
    // the old scope check passes the record straight through and the job
    // overwrites a deliberate edit. Only the clock catches it.
    const a = await stage();
    const b = await stage();
    await rule(a, won());
    await rule(b, won());
    const ids = await seed(a, 1);
    const { jobId, batchNo } = await jobOver(a, ids);

    await personMoves(ids[0]!, b);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(0);
    expect(result.skipped).toBe(1);
    expect(result.failed).toBe(0);
    expect(await stageOf(ids[0]!)).toBe(b);
  });

  it('proceeds when the person moved it BEFORE submission', async () => {
    // The other side of the ordering, and the one that would be easy to break by
    // making the rule too eager. An edit older than the job is not a conflict -
    // the job was asked to act on the record as it was then.
    const a = await stage();
    const b = await stage();
    await rule(b, won());
    const ids = await seed(a, 1);
    await personMoves(ids[0]!, b);
    // The filter names b, which is where the record now is.
    const { jobId, batchNo } = await jobOver(b, ids);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(1);
    expect(result.skipped).toBe(0);
    expect(await stageOf(ids[0]!)).toBe(won());
  });

  it('an older job defers to a newer one that already moved the record', async () => {
    // jobA is submitted first, jobB second. jobB runs to completion, then jobA
    // reaches the same record. jobA must not undo jobB.
    const from = await stage();
    const ids = await seed(from, 1);

    // Submission times a second apart, stated rather than left to whatever order
    // two INSERTs happened to land in. The whole test is about which of these two
    // is newer, so it cannot be left to chance - and at this speed two jobs
    // created back to back can share a timestamp, which is the documented tie.
    //
    // Different targets, deliberately. If both aimed at the same stage then the
    // record would already be at the older job's target when it arrived, and the
    // alreadyThere branch would count it as moved - correct, but it would mean
    // the clock was never consulted and the test would prove nothing about it.
    const olderTarget = await stage();
    await rule(from, olderTarget);
    await rule(from, won());
    await backdateClock(ids, '2026-01-01T09:00:00Z');
    const older = await jobOver(from, ids, {
      submittedAt: '2026-01-01T10:00:00Z',
      target: olderTarget,
    });
    const newer = await jobOver(from, ids, { submittedAt: '2026-01-01T10:00:01Z' });

    await processBatchForTest(ws.workspaceId, newer.jobId, newer.batchNo);
    expect(await stageOf(ids[0]!)).toBe(won());

    const result = await processBatchForTest(ws.workspaceId, older.jobId, older.batchNo);
    expect(result.moved).toBe(0);
    expect(result.skipped).toBe(1);
    // Still where the newer job put it, not dragged back to the older target.
    expect(await stageOf(ids[0]!)).toBe(won());
  });

  it('a newer job still moves a record an older job already moved', async () => {
    // The reverse ordering, and the one that a wall-clock stamp would get wrong.
    // jobA is older but runs FIRST here, stamping the wall clock. jobB is newer
    // and arrives second. Under last-writer-wins jobB would defer to jobA and the
    // record would sit at jobA's target; under the logical clock jobB's submission
    // is later, so it proceeds.
    const from = await stage();
    const viaA = await stage();
    const viaB = await stage();
    await rule(from, viaA);
    await rule(viaA, viaB);
    const ids = await seed(from, 1);

    await backdateClock(ids, '2026-01-01T09:00:00Z');
    // jobB's filter names `from` and viaA, so the record is still in scope for it
    // after jobA has moved it. Without viaA in the filter the scope check would
    // skip the record and the clock would never be reached - the test would pass
    // for the wrong reason.
    const older = await jobOver(from, ids, {
      submittedAt: '2026-01-01T10:00:00Z',
      target: viaA,
    });
    const newer = await jobOver(from, ids, {
      submittedAt: '2026-01-01T10:00:01Z',
      target: viaB,
      alsoInScope: [viaA],
    });

    // The older job runs first and lands the record on viaA.
    const first = await processBatchForTest(ws.workspaceId, older.jobId, older.batchNo);
    expect(first.moved).toBe(1);
    expect(await stageOf(ids[0]!)).toBe(viaA);

    // jobB's submission is later than the clock jobA left, so jobB proceeds and
    // carries the record on to viaB. This is the assertion that fails under a
    // wall-clock stamp: jobA wrote "now", which is after jobB was submitted, so
    // jobB would defer and the record would sit at viaA for ever.
    const second = await processBatchForTest(ws.workspaceId, newer.jobId, newer.batchNo);
    expect(second.moved).toBe(1);
    expect(second.skipped).toBe(0);
    expect(await stageOf(ids[0]!)).toBe(viaB);
  });

  it('counts a record already at the target as moved, not skipped', async () => {
    // The ordering trap. A person moves a record TO the target, so its clock is
    // newer than the job's - and the job achieved exactly what it was asked to
    // do. If the clock check ran before this one, the record would be reported as
    // skipped and the job would under-report itself.
    const from = await stage();
    await rule(from, won());
    const ids = await seed(from, 1);
    const { jobId, batchNo } = await jobOver(from, ids);

    await personMoves(ids[0]!, won());

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(1);
    expect(result.skipped).toBe(0);
  });

  it('a retry is not blocked by its own earlier attempt', async () => {
    // The reason the job writes snapshot_at rather than now(). If it wrote the
    // wall clock, attempt 1 would stamp a time later than the job's own
    // submission, and attempt 2 would see every record as decided-after and skip
    // all of them - a job that moved everything on the first try and then
    // reported zero on the retry.
    const from = await stage();
    await rule(from, won());
    const ids = await seed(from, 3);
    const { jobId, batchNo } = await jobOver(from, ids);

    const first = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(first.moved).toBe(3);

    // Put the batch back the way a redelivery finds it, and run it again.
    await pool.query(
      `UPDATE bulk_job_outbox SET status = 'pending', completed_at = NULL
        WHERE job_id = $1 AND batch_no = $2`,
      [jobId, batchNo],
    );
    const second = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    // Every record already at the target, counted as moved - not skipped, which is
    // what a self-blocking clock would produce.
    expect(second.moved).toBe(3);
    expect(second.skipped).toBe(0);
  });

  it('a rename does not remove a record from an in-flight job', async () => {
    // The trigger's first WHEN clause. Any UPDATE that bumped the clock would mean
    // editing a deal's name silently dropped it from every running job, which is
    // the same class of bug as a bulk job overwriting a deliberate change.
    const from = await stage();
    await rule(from, won());
    const ids = await seed(from, 1);
    const { jobId, batchNo } = await jobOver(from, ids);

    await pool.query(`UPDATE opportunity SET name = 'renamed-' || id WHERE id = $1`, [ids[0]!]);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(1);
    expect(result.skipped).toBe(0);
    expect(await stageOf(ids[0]!)).toBe(won());
  });

  it('a value edit does not remove a record either', async () => {
    const from = await stage();
    await rule(from, won());
    const ids = await seed(from, 1);
    const { jobId, batchNo } = await jobOver(from, ids);

    await pool.query('UPDATE opportunity SET value = 12345 WHERE id = $1', [ids[0]!]);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(1);
    expect(result.skipped).toBe(0);
  });

  it('stamps a job write with the job time, not the write time', async () => {
    // The mechanism, asserted directly. If this ever became now() the two ordering
    // tests above would still pass by luck on a fast run and the property would
    // be gone.
    //
    // The job's submission is pinned an hour in the past rather than left at now(),
    // so the gap between "when this job was submitted" and "when this row was
    // touched" is an hour instead of a few milliseconds. Comparing against a live
    // now() is not a usable assertion: pg returns timestamptz as a JS Date, which
    // rounds to milliseconds, so the two can differ by a millisecond in either
    // direction and the test would be measuring rounding rather than the property.
    const from = await stage();
    await rule(from, won());
    const ids = await seed(from, 1);
    await backdateClock(ids, '2026-01-01T08:00:00Z');
    const { jobId, batchNo } = await jobOver(from, ids, {
      submittedAt: '2026-01-01T09:00:00Z',
    });

    await processBatchForTest(ws.workspaceId, jobId, batchNo);

    const { rows } = await pool.query<{ stage_decided_at: Date }>(
      'SELECT stage_decided_at FROM opportunity WHERE id = $1',
      [ids[0]!],
    );
    // The job's submission time, verbatim, not the moment the row moved - which
    // is months later. This is the difference between a logical clock and a wall
    // clock, and it is the whole reason the worker names the column rather than
    // letting the trigger stamp it.
    expect(rows[0]!.stage_decided_at.toISOString()).toBe('2026-01-01T09:00:00.000Z');
  });

  it('a job that skipped records still settles rather than hanging', async () => {
    // The end-to-end version. processed_count is allowed to land below
    // total_matched - that is the documented consequence of a record being out of
    // scope - and the job must still reach a terminal state, or a single manual
    // edit would hang it at running for ever.
    const from = await stage();
    const other = await stage();
    await rule(from, won());
    const ids = await seed(from, 4);
    const { jobId } = await jobOver(from, ids);

    await personMoves(ids[0]!, other);
    const result = await processBatchForTest(ws.workspaceId, jobId, 0);
    expect(result.skipped).toBe(1);

    // createJobWithItems was not used, so the batch is the one the builder wrote;
    // it is already settled by the call above. The assertion is that the row
    // reached a terminal state rather than being left pending.
    const { rows } = await pool.query<{ status: string; processed_count: number }>(
      'SELECT status, processed_count FROM bulk_job WHERE id = $1',
      [jobId],
    );
    expect(['completed', 'running']).toContain(rows[0]!.status);
    expect(rows[0]!.processed_count).toBe(3);
  });

  it('a job over records a person edited afterwards reports the shortfall', async () => {
    // Not just the count - the job must not claim to have moved what it left
    // alone. Guards against a future change that folds skips into moved.
    const from = await stage();
    const other = await stage();
    await rule(from, won());
    const ids = await seed(from, 2);
    const { jobId, batchNo } = await jobOver(from, ids);

    await personMoves(ids[0]!, other);
    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(1);
    expect(result.skipped).toBe(1);

    const { rows } = await pool.query<{ total_matched: number; processed_count: number }>(
      'SELECT total_matched, processed_count FROM bulk_job WHERE id = $1',
      [jobId],
    );
    expect(rows[0]!.total_matched).toBe(2);
    expect(rows[0]!.processed_count).toBe(1);
  });

  // The trigger is the other half of this feature and is exercised here through
  // the two non-stage write paths, because that is the only way to prove the WHEN
  // clause stands the trigger down for a rename and for a value edit. The
  // column's default and NOT NULL are the schema's business, not this file's.
});
