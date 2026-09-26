import { randomUUID } from 'node:crypto';
import {
  BASE,
  api,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  recordJobTransition,
  seedOpportunities,
  submitBulkMove,
  type TestWorkspace,
} from '../helpers';

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
      const { rows } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM bulk_job
         WHERE workspace_id = $1 AND status = 'pending'`,
        [ws.workspaceId],
      );
      expect(rows[0]!.n).toBeGreaterThan(1);
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

  describe('items are scoped to the job tenant', () => {
    it('refuses an item whose opportunity belongs to another workspace', async () => {
      const other = await provisionWorkspace('bulkjob-xw');
      const job = await submitBulkMove(other.workspaceId, {
        targetStageId: other.stages['newLead'],
      });
      await expect(
        pool.query(
          `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id)
           VALUES ($1, $2, $3)`,
          [job.body.jobId, other.workspaceId, randomUUID()],
        ),
      ).rejects.toThrow();
      await destroyWorkspace(other.workspaceId);
    });

    it('refuses the same opportunity twice in one job, so a job cannot double-apply', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT opportunity_id FROM bulk_job_item WHERE job_id = $1 LIMIT 1',
        [job.body.jobId],
      );
      await expect(
        pool.query(
          `INSERT INTO bulk_job_item (job_id, workspace_id, opportunity_id)
           VALUES ($1, $2, $3)`,
          [job.body.jobId, ws.workspaceId, rows[0]!.opportunity_id],
        ),
      ).rejects.toThrow(/bulk_job_item_job_opportunity_uniq/);
    });

    it('deleting a job removes its items', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      await pool.query('DELETE FROM bulk_job WHERE id = $1', [job.body.jobId]);
      const { rows } = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM bulk_job_item WHERE job_id = $1',
        [job.body.jobId],
      );
      expect(rows[0]!.n).toBe(0);
    });
  });

  describe('transitions are attributable to a job', () => {
    it('a job transition is listed and a manual move is not', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT opportunity_id FROM bulk_job_item WHERE job_id = $1 LIMIT 2',
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
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT opportunity_id FROM bulk_job_item WHERE job_id = $1 LIMIT 1',
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
