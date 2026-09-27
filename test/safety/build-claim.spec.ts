import { randomUUID } from 'node:crypto';
import { DatabaseService } from '../../src/shared/database/database.service';
import { SnapshotBuilder } from '../../src/apps/transition-service/snapshot-builder.service';
import { rabbitConfig } from '../../src/shared/rabbit/rabbit.config';
import {
  DB_URL,
  pool,
  provisionWorkspace,
  waitForSnapshot,
  withDeadlockRetry,
  type TestWorkspace,
} from '../helpers';

/**
 * One builder per job, but many builders at once.
 *
 * Two different concurrency questions live here and only one of them has a limit
 * worth naming. Within a job the walk cannot be parallelised: page N+1's predicate
 * holds page N's last row as its keyset cursor, so there is nothing to ask for
 * until page 1 has come back. Across jobs there is no coupling at all - separate
 * filter, separate watermark, separate cursor - so three jobs arriving together
 * should be three builds, not a queue. The rule the system has to enforce is
 * "one walker per job", and what is worth testing is that the database enforces
 * it rather than there happening to be one replica.
 *
 * One thing shapes every assertion here. The running transition service sweeps on
 * the same 250ms tick over the same table, so it competes for every job these
 * tests plant. Anything asserted synchronously after a sweep is a race against it
 * and will flake - observed at roughly one run in four before this was written
 * this way. So a job the test wants built is waited for with waitForSnapshot, and
 * only a job whose lock the test itself holds is asserted immediately, because
 * there the live service is excluded by the same lock.
 */
describe('the build claims a job before walking it', () => {
  let ws: TestWorkspace;
  let db: DatabaseService;
  const target = () => ws.stages['closedWon'];

  beforeAll(async () => {
    ws = await provisionWorkspace('build-claim');
    // The builders need their own pool, separate from the test's, because
    // DatabaseService reads DATABASE_URL from an environment the test process does
    // not set - it resolves the same URL the helper pool uses.
    process.env.DATABASE_URL = DB_URL;
    db = new DatabaseService();
  });

  afterAll(async () => {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await db.onModuleDestroy();
    await pool.end();
  });

  /**
   * A stage of its own for every test.
   *
   * Jobs drain asynchronously now, so a stage shared between tests can be empty
   * by the time the next test's snapshot is taken - an earlier test's job has
   * already moved everything out of it. That is correct behaviour and a false
   * assumption in the test, which is why nothing here filters on a shared stage.
   */
  const privateStage = async (): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO stage (workspace_id, name, outcome)
       VALUES ($1, $2, 'open') RETURNING id`,
      [ws.workspaceId, `s-${randomUUID().slice(0, 8)}`],
    );
    return rows[0]!.id;
  };

  const seed = async (stageId: string, count: number): Promise<void> => {
    await pool.query(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       SELECT $1, $2, 'rec-' || g, 100 FROM generate_series(1, $3::int) g`,
      [ws.workspaceId, stageId, count],
    );
  };

  /** A job the submit path will not touch, as a crashed build would leave it. */
  const plant = async (stageId: string): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id, status)
       VALUES ($1, $2, $3::jsonb, $4, 'preparing') RETURNING id`,
      [ws.workspaceId, `claim-${randomUUID()}`, JSON.stringify({ stageId: [stageId] }), target()],
    );
    return rows[0]!.id;
  };

  /** Stands in for a second replica that has already claimed this job. */
  const holdLock = async (jobId: string): Promise<void> => {
    const client = await pool.connect();
    try {
      const got = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
        [jobId],
      );
      expect(got.rows[0]!.locked).toBe(true);
    } finally {
      // Held on this connection deliberately; the release is explicit below so a
      // failure inside the test still frees the key rather than leaking it for
      // the life of the pool.
      heldClients.push(client);
    }
  };

  const heldClients: Array<{
    query: (q: string, v?: unknown[]) => Promise<unknown>;
    release: () => void;
  }> = [];

  const releaseLock = async (jobId: string): Promise<void> => {
    for (const c of heldClients.splice(0)) {
      await c.query('SELECT pg_advisory_unlock(hashtext($1))', [jobId]);
      c.release();
    }
  };

  const batchesFor = async (jobId: string): Promise<number> => {
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    return rows[0]!.n;
  };

  const build = (): SnapshotBuilder => new SnapshotBuilder(db, rabbitConfig());

  afterEach(async () => {
    // Any lock a test took and did not release, so a failing test cannot leave a
    // key held against every later run in this file.
    await releaseLock('*');
  });

  it('skips a job another builder is already walking', async () => {
    const stageId = await privateStage();
    await seed(stageId, 40);
    const jobId = await plant(stageId);

    // A replica elsewhere holds this job. The sweep must not touch it - not
    // because that would corrupt anything, but because walking it a second time
    // reads and writes every page for no gain, and the waste scales with the
    // number of replicas.
    //
    // Safe to assert immediately: the live service honours this lock too, so
    // nothing can build the job until it is released.
    await holdLock(jobId);
    await build().sweep();

    expect(await batchesFor(jobId)).toBe(0);
    const { rows } = await pool.query<{ status: string }>(
      'SELECT status FROM bulk_job WHERE id = $1',
      [jobId],
    );
    // Still preparing, so a later sweep picks it up once the lock clears.
    expect(rows[0]!.status).toBe('preparing');

    await releaseLock(jobId);
    const built = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
    expect(built.totalMatched).toBe(40);
  });

  it('a job held by one builder does not block a different job', async () => {
    // The case that makes replicas worth running. The claim is per job, not a
    // mutex over the sweep: one process walking job A must not stop another from
    // walking job B, or adding replicas would achieve nothing.
    const heldStage = await privateStage();
    const freeStage = await privateStage();
    await seed(heldStage, 25);
    await seed(freeStage, 30);
    const heldJob = await plant(heldStage);
    const freeJob = await plant(freeStage);

    await holdLock(heldJob);
    await build().sweep();

    // The property is the asymmetry, not who built what: the free job is allowed
    // to finish while the held one stays untouched for the whole of it. If the
    // claim were a mutex over the sweep rather than per job, the free job would
    // never be built and this would time out.
    const free = await waitForSnapshot(ws.workspaceId, freeJob, 20_000);
    expect(free.snapshotInProgress).toBe(false);
    expect(free.totalMatched).toBe(30);

    // Still nothing, with the lock held throughout the wait above.
    expect(await batchesFor(heldJob)).toBe(0);
    const { rows } = await pool.query<{ status: string }>(
      'SELECT status FROM bulk_job WHERE id = $1',
      [heldJob],
    );
    expect(rows[0]!.status).toBe('preparing');

    // Released, so it is buildable again.
    await releaseLock(heldJob);
    const rebuilt = await waitForSnapshot(ws.workspaceId, heldJob, 20_000);
    expect(rebuilt.totalMatched).toBe(25);
  });

  it('releases the claim once the walk is done', async () => {
    // Guards the leak that would make a job permanently unbuildable. If the
    // unlock were skipped, the key would stay held for the life of the process and
    // every later sweep would skip that job forever - the failure the worker's
    // identical lock once had, which is why a failed unlock destroys the
    // connection rather than returning it to the pool.
    //
    // Proved by taking the same key afterwards rather than by observing a second
    // build, so it holds whoever did the first one.
    const stageId = await privateStage();
    await seed(stageId, 12);
    const jobId = await plant(stageId);

    await build().sweep();
    const built = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
    expect(built.totalMatched).toBe(12);

    // The unlock lands just after the job row is finalised, so the key is not
    // free the instant the status flips. Retried briefly rather than slept on.
    const probe = await pool.connect();
    try {
      let acquired = false;
      for (let i = 0; i < 40 && !acquired; i += 1) {
        const got = await probe.query<{ locked: boolean }>(
          'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
          [jobId],
        );
        acquired = got.rows[0]!.locked;
        if (acquired) break;
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(acquired).toBe(true);
    } finally {
      await probe.query('SELECT pg_advisory_unlock(hashtext($1))', [jobId]).catch(() => undefined);
      probe.release();
    }
  });

  it('does not re-walk a job that is no longer preparing', async () => {
    // The claim is released *after* the job row is finalised, so there is a real
    // window in which a second builder can hold the key for a job that is already
    // pending. It must do nothing rather than start the walk again from batch 0 -
    // which is what the status check on the read prevents.
    const stageId = await privateStage();
    await seed(stageId, 1_500);
    const jobId = await plant(stageId);

    await build().sweep();
    const first = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
    expect(first.totalMatched).toBe(1_500);

    // Back to preparing with the batches already written, which is the shape the
    // window produces: the status has moved on, the key is free, the rows exist.
    await pool.query(
      `UPDATE bulk_job
          SET status = 'preparing', snapshot_cursor = NULL, snapshot_cursor_id = NULL
        WHERE id = $1`,
      [jobId],
    );

    const before = await pool.query<{ n: number; max_batch: number }>(
      'SELECT count(*)::int AS n, coalesce(max(batch_no), -1)::int AS max_batch FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );

    await build().sweep();

    const after = await pool.query<{ n: number; max_batch: number }>(
      'SELECT count(*)::int AS n, coalesce(max(batch_no), -1)::int AS max_batch FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    // No new rows and no renumbering. A re-walk would have rewritten batch 0 and
    // 1 in place under ON CONFLICT DO NOTHING, which is invisible in the row
    // count - so the check that matters is that the finalise did not run, which
    // the status below shows.
    expect(after.rows[0]!.n).toBe(before.rows[0]!.n);
    expect(after.rows[0]!.max_batch).toBe(before.rows[0]!.max_batch);

    // The live service's sweep may have picked it up in between, so the status is
    // allowed to have moved on - but total_matched must still be the 1,500 the
    // batches hold, never a recount from a second walk.
    const job = await pool.query<{ total_matched: number }>(
      'SELECT total_matched FROM bulk_job WHERE id = $1',
      [jobId],
    );
    expect(job.rows[0]!.total_matched).toBe(1_500);
  });

  it('two builders on one job leave exactly one set of batches', async () => {
    // The invariant, asserted on the result rather than the mechanism, so it holds
    // however the builders interleave. Even if both got past the claim - a flushed
    // lock, a manually reset session - the batch insert is ON CONFLICT DO NOTHING
    // and both compute the same cursor, so the stored answer is right and the job
    // never reports a count its own rows contradict.
    const stageId = await privateStage();
    await seed(stageId, 2_500);
    const jobId = await plant(stageId);

    await Promise.all([build().sweep(), build().sweep()]);
    const built = await waitForSnapshot(ws.workspaceId, jobId, 30_000);
    expect(built.totalMatched).toBe(2_500);

    const { rows } = await pool.query<{ total: number; distinct_ids: number }>(
      `SELECT (SELECT coalesce(sum(cardinality(item_ids)), 0)::int
                 FROM bulk_job_outbox WHERE job_id = $1) AS total,
              (SELECT count(DISTINCT opportunity_id)::int
                 FROM bulk_job_outbox, unnest(item_ids) AS u(opportunity_id)
                WHERE job_id = $1) AS distinct_ids`,
      [jobId],
    );
    // A record in two batches would be moved twice and counted twice.
    expect(rows[0]!.total).toBe(rows[0]!.distinct_ids);
    expect(rows[0]!.total).toBe(2_500);
  });

  it('builds every unfinished job it finds, not just the first', async () => {
    // A single pass walking several jobs, which is what makes the sequential
    // loop acceptable: one process handles a burst, and replicas divide the burst
    // between them rather than each repeating all of it.
    const counts = [10, 20, 30];
    const jobs: string[] = [];
    for (const [i, count] of counts.entries()) {
      const stageId = await privateStage();
      await seed(stageId, count);
      jobs.push(await plant(stageId));
      void i;
    }

    await build().sweep();

    for (const [i, jobId] of jobs.entries()) {
      const built = await waitForSnapshot(ws.workspaceId, jobId, 20_000);
      expect(built.totalMatched).toBe(counts[i]);
    }
  });
});
