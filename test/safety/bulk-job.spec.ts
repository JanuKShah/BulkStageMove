import { randomUUID } from 'node:crypto';
import {
  BASE,
  api,
  createJobWithItems,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  recordJobTransition,
  resetBatchTo,
  seedOpportunities,
  seedOpportunitiesInStage,
  submitBulkMove,
  waitForSnapshot,
  type TestWorkspace,
} from '../helpers';
import { closeWorkerHarness, processBatchForTest } from './worker-harness';

/**
 * The mechanisms that make a bulk job safe to retry and safe to attribute.
 * Each case goes red if the corresponding constraint or check is removed.
 */
describe('bulk job safety', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('bulkjob');
    await seedOpportunities(ws.workspaceId, ws.stages['newLead'], 40);
    await seedOpportunities(ws.workspaceId, ws.stages['contacted'], 25);
  });

  afterAll(async () => {
    await closeWorkerHarness();
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  const newLead = () => ws.stages['newLead'];
  const contacted = () => ws.stages['contacted'];

  describe('idempotency is keyed on the caller key, not the filter', () => {
    it('a retry with the same key returns the same job and creates nothing', async () => {
      const key = randomUUID();
      const first = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      expect(first.status).toBe(201);
      expect(first.body.replay).toBe(false);

      const retry = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      // Same status as the original. The body carries the distinction, so the
      // status does not have to - and must not - change between the two.
      expect(retry.status).toBe(201);
      expect(retry.body.jobId).toBe(first.body.jobId);
      expect(retry.body.replay).toBe(true);
      expect(retry.body.itemsCreated).toBe(0);
    });

    it('the same key with a different target stage is rejected', async () => {
      const key = randomUUID();
      await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      const clash = await submitBulkMove(ws.workspaceId, { targetStageId: contacted() }, key);
      expect(clash.status).toBe(409);
    });

    it('the same key with a different filter is rejected', async () => {
      const key = randomUUID();
      await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      const clash = await submitBulkMove(
        ws.workspaceId,
        { targetStageId: newLead(), outcome: 'won' },
        key,
      );
      expect(clash.status).toBe(409);
    });

    it('a deliberate re-run with a NEW key and the same filter is a new job', async () => {
      const first = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const rerun = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      expect(rerun.status).toBe(201);
      expect(rerun.body.replay).toBe(false);
      expect(rerun.body.jobId).not.toBe(first.body.jobId);
    });

    it('several unfinished jobs may exist in one workspace', async () => {
      // Counted as anything not yet terminal, and waited for rather than
      // sampled. A job is created 'preparing' and only becomes 'pending' once its
      // batches are built, which is up to a couple of seconds after the response -
      // so counting 'pending' alone was reading a race, and returned 1 whenever
      // the build had not finished. The property under test is that the uniqueness
      // key is (workspace_id, idempotency_key) rather than the workspace, so what
      // matters is that none of these are finished.
      const deadline = Date.now() + 20_000;
      for (;;) {
        const { rows } = await pool.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM bulk_job
            WHERE workspace_id = $1
              AND status NOT IN ('completed', 'failed')`,
          [ws.workspaceId],
        );
        if (rows[0]!.n > 1) break;
        if (Date.now() > deadline) {
          throw new Error(`only ${rows[0]!.n} unfinished job(s) after 20s`);
        }
        await new Promise((r) => setTimeout(r, 50));
      }
    });

    it('the database refuses a duplicate key even if the service check were removed', async () => {
      const key = randomUUID();
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      await expect(
        pool.query(
          `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id)
           VALUES ($1, $2, '{}', $3)`,
          [ws.workspaceId, key, newLead()],
        ),
      ).rejects.toThrow(/bulk_job_workspace_idempotency_uniq/);
      expect(job.status).toBe(201);
    });

    it('the same key is free in a different workspace', async () => {
      const other = await provisionWorkspace('bulkjob-other');
      const key = randomUUID();
      const a = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() }, key);
      const b = await submitBulkMove(
        other.workspaceId,
        { targetStageId: other.stages['newLead'] },
        key,
      );
      expect(a.status).toBe(201);
      expect(b.status).toBe(201);
      await destroyWorkspace(other.workspaceId);
    });
  });

  describe('batches are scoped to the job tenant', () => {
    it('refuses a batch whose job belongs to another workspace', async () => {
      const other = await provisionWorkspace('bulkjob-xw');
      const job = await submitBulkMove(other.workspaceId, {
        targetStageId: other.stages['newLead'],
      });
      // A real id is supplied so the failure is the composite FK, not a NOT NULL
      // or type violation - otherwise a generic toThrow() passes for the wrong
      // reason. bulk_job_outbox carries (job_id, workspace_id) as a composite
      // reference, which is what makes a cross-tenant batch impossible to write.
      await expect(
        pool.query(
          `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
           VALUES ($1, $2, 99, $3::uuid[])`,
          [ws.workspaceId, job.body.jobId, [randomUUID()]],
        ),
      ).rejects.toThrow(/bulk_job_outbox_job_fk|foreign key/);
      await destroyWorkspace(other.workspaceId);
    });

    it('a retry does not write a second transition for a record it already moved', async () => {
      // Not a constraint test. Retry safety comes from the batch being one
      // transaction and from the worker skipping records already at the target,
      // so it has to be exercised by running processBatch twice. There is no
      // unique index here to assert against, deliberately - see 0001_schema.sql
      // for why one is not needed and what would have to change to bring it back.
      // contacted -> closedWon, because the fixture only permits forward moves
      // and a backward one would correctly fail as unmovable, testing nothing
      // about retry.
      const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 5);
      const target = ws.stages['closedWon'];
      const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, target, ids, {
        enqueue: false,
      });

      const first = await processBatchForTest(ws.workspaceId, jobId, batchNo);
      expect(first.moved).toBe(5);
      expect(first.failed).toBe(0);

      // Put the batch back to claimable, exactly as a redelivered message would
      // find one whose worker died after committing.
      await resetBatchTo(jobId, batchNo, 'pending');
      const second = await processBatchForTest(ws.workspaceId, jobId, batchNo);

      const { rows } = await pool.query<{ n: number; dupes: number }>(
        `SELECT count(*)::int AS n,
                (SELECT count(*)::int FROM (
                   SELECT opportunity_id FROM opportunity_transition
                    WHERE job_id = $1
                    GROUP BY opportunity_id HAVING count(*) > 1
                 ) d) AS dupes
           FROM opportunity_transition WHERE job_id = $1`,
        [jobId],
      );
      expect(rows[0]!.n).toBe(5);
      expect(rows[0]!.dupes).toBe(0);
      // The retry finds every record already where it wanted them, so it reports
      // them as its own settled work rather than as new moves or as failures.
      expect(second.moved).toBe(5);
      expect(second.failed).toBe(0);

      // And the job's counters do not double-count. They are re-derived from the
      // batch rows rather than incremented, precisely so a batch that settles
      // twice still reports its records once - an increment here reported 10 for
      // a 5 record job.
      const counters = await pool.query<{ processed_count: number; total_matched: number }>(
        'SELECT processed_count, total_matched FROM bulk_job WHERE id = $1',
        [jobId],
      );
      expect(counters.rows[0]!.processed_count).toBe(5);
      expect(counters.rows[0]!.processed_count).toBe(counters.rows[0]!.total_matched);
    });

    it('deleting a job removes its batches and its failures', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      await pool.query(
        `INSERT INTO bulk_job_failure (workspace_id, job_id, batch_no, opportunity_id, error)
         SELECT $1, $2, 0, unnest(item_ids), 'forced' FROM bulk_job_outbox
          WHERE job_id = $2 LIMIT 1`,
        [ws.workspaceId, job.body.jobId],
      );
      await pool.query('DELETE FROM bulk_job WHERE id = $1', [job.body.jobId]);
      const { rows } = await pool.query<{ batches: number; failures: number }>(
        `SELECT (SELECT count(*)::int FROM bulk_job_outbox WHERE job_id = $1) AS batches,
                (SELECT count(*)::int FROM bulk_job_failure WHERE job_id = $1) AS failures`,
        [job.body.jobId],
      );
      expect(rows[0]!.batches).toBe(0);
      expect(rows[0]!.failures).toBe(0);
    });
  });

  describe('transitions are attributable to a job', () => {
    it('a job transition is listed and a manual move is not', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      // The batches have to exist before their ids can be read. Submission returns
      // as soon as the job row does, so the outbox is still empty at this point -
      // this read used to find nothing and fail on rows[1] being undefined, or
      // silently assert against a single id.
      await waitForSnapshot(ws.workspaceId, job.body.jobId);
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 LIMIT 2',
        [job.body.jobId],
      );
      const first = rows[0]!;
      const second = rows[1]!;
      await recordJobTransition(
        ws.workspaceId,
        job.body.jobId,
        first.opportunity_id,
        contacted(),
        newLead(),
      );
      await recordJobTransition(
        ws.workspaceId,
        job.body.jobId,
        second.opportunity_id,
        contacted(),
        newLead(),
      );

      // a manual move on some other opportunity in the same workspace
      await pool.query(
        `INSERT INTO opportunity_transition (workspace_id, opportunity_id, to_stage_id)
         SELECT $1, id, $2 FROM opportunity
         WHERE workspace_id = $1 AND id <> ALL($3::uuid[]) LIMIT 1`,
        [ws.workspaceId, newLead(), [first.opportunity_id, second.opportunity_id]],
      );

      const listed = await api<{ items: { opportunity_id: string }[] }>(
        BASE.transition,
        `/bulk-moves/${job.body.jobId}/transitions?limit=200`,
        { workspaceId: ws.workspaceId },
      );
      expect(listed.body.items).toHaveLength(2);
      expect(
        listed.body.items.every((t) =>
          [first.opportunity_id, second.opportunity_id].includes(t.opportunity_id),
        ),
      ).toBe(true);
    });

    it('deleting a job keeps the transition and clears job_id', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      await waitForSnapshot(ws.workspaceId, job.body.jobId);
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 LIMIT 1',
        [job.body.jobId],
      );
      const opportunityId = rows[0]!.opportunity_id;
      const transitionId = await recordJobTransition(
        ws.workspaceId,
        job.body.jobId,
        opportunityId,
        null,
        newLead(),
      );

      await pool.query('DELETE FROM bulk_job WHERE id = $1', [job.body.jobId]);

      const after = await pool.query<{ job_id: string | null }>(
        'SELECT job_id FROM opportunity_transition WHERE id = $1',
        [transitionId],
      );
      expect(after.rows[0]!.job_id).toBeNull();
    });

    it('another workspace cannot read a job or its transitions', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const other = await provisionWorkspace('bulkjob-peek');
      for (const path of ['', '/transitions']) {
        const res = await api<unknown>(BASE.transition, `/bulk-moves/${job.body.jobId}${path}`, {
          workspaceId: other.workspaceId,
        });
        expect(res.status).toBe(404);
      }
      await destroyWorkspace(other.workspaceId);
    });
  });
});
