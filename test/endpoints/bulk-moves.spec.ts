import { randomUUID } from 'node:crypto';
import {
  BASE,
  type ApiResponseLike,
  api,
  destroyWorkspace,
  insertTransitionWithMicros,
  pool,
  provisionWorkspace,
  recordJobTransition,
  seedOpportunities,
  submitBulkMove,
  waitForJobSettled,
  waitForSnapshot,
  type TestWorkspace,
} from '../helpers';

describe('bulk-moves endpoints', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('bulkep');
    await seedOpportunities(ws.workspaceId, ws.stages['newLead'], 30);
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  const newLead = () => ws.stages['newLead'];

  describe('POST /bulk-moves', () => {
    it('creates a job with one item per matching opportunity', async () => {
      const expected = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1',
        [ws.workspaceId],
      );
      const res = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      expect(res.status).toBe(201);
      const snap = await waitForSnapshot(ws.workspaceId, res.body.jobId);
      expect(snap.totalMatched).toBe(expected.rows[0]!.n);

      const items = await pool.query<{ n: number }>(
        'SELECT coalesce(sum(cardinality(item_ids)), 0)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
        [res.body.jobId],
      );
      expect(items.rows[0]!.n).toBe(snap.totalMatched);
    });

    it('total_matched agrees with the records written into batches', async () => {
      const res = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const job = await pool.query<{ total_matched: number; records: number }>(
        `SELECT j.total_matched,
                coalesce((SELECT sum(cardinality(b.item_ids))::int
                            FROM bulk_job_outbox b WHERE b.job_id = j.id), 0) AS records
           FROM bulk_job j WHERE j.id = $1`,
        [res.body.jobId],
      );
      expect(job.rows[0]!.total_matched).toBe(job.rows[0]!.records);
    });

    it('filters the snapshot rather than taking the whole workspace', async () => {
      const res = await submitBulkMove(ws.workspaceId, {
        targetStageId: newLead(),
        outcome: 'won',
      });
      const expected = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM opportunity o
         JOIN stage s ON s.id = o.stage_id
         WHERE o.workspace_id = $1 AND s.outcome = 'won'`,
        [ws.workspaceId],
      );
      const snap = await waitForSnapshot(ws.workspaceId, res.body.jobId);
      expect(snap.totalMatched).toBe(expected.rows[0]!.n);
    });

    // Built inside the test, not in the table literal: the table is evaluated
    // at collection time, before beforeAll has provisioned a workspace.
    it.each([
      ['a missing idempotencyKey', (_id: string) => ({}), 'idempotencyKey'],
      ['a missing targetStageId', () => ({ targetStageId: null }), 'targetStageId'],
      ['a malformed targetStageId', () => ({ targetStageId: 'not-a-uuid' }), 'targetStageId'],
    ])('rejects %s with 400', async (_label, build, omit) => {
      const body: Record<string, unknown> = {
        idempotencyKey: randomUUID(),
        ...build(newLead()),
      };
      delete body[omit];
      const res = await api<{ message: string }>(BASE.transition, '/bulk-moves', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(400);
    });

    it('rejects an unknown filter key rather than ignoring it', async () => {
      const res = await api<{ message: string }>(BASE.transition, '/bulk-moves', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({
          idempotencyKey: randomUUID(),
          targetStageId: newLead(),
          sortBy: 'value',
        }),
      });
      expect(res.status).toBe(400);
      expect(res.body.message).toMatch(/unknown filter/);
    });

    it('requires the workspace header', async () => {
      const res = await api<{ message: string }>(BASE.transition, '/bulk-moves', {
        method: 'POST',
        body: JSON.stringify({ idempotencyKey: randomUUID(), targetStageId: newLead() }),
      });
      expect(res.status).toBe(400);
      expect(res.body.message).toMatch(/x-workspace-id/);
    });

    it('a filter matching nothing still produces a job with zero items', async () => {
      const res = await submitBulkMove(ws.workspaceId, {
        targetStageId: newLead(),
        minValue: 999_999_999,
      });
      expect(res.status).toBe(201);
      // A job that matched nothing has no batches, so nothing would ever settle
      // it. The build marks it completed rather than leaving it pending for ever.
      const snap = await waitForSnapshot(ws.workspaceId, res.body.jobId);
      expect(snap.totalMatched).toBe(0);
      expect(snap.status).toBe('completed');
    });

    it('JSON key order does not change the job identity', async () => {
      const key = randomUUID();
      const a = await submitBulkMove(
        ws.workspaceId,
        { targetStageId: newLead(), outcome: 'won' },
        key,
      );
      const b = await api<{ jobId: string; replay: boolean }>(BASE.transition, '/bulk-moves', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ outcome: 'won', targetStageId: newLead(), idempotencyKey: key }),
      });
      expect(b.body.jobId).toBe(a.body.jobId);
      expect(b.body.replay).toBe(true);
    });
  });

  describe('GET /bulk-moves/:id', () => {
    it('reports batch counts, and the watermark the counts are as of', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      // Before reading any batch row. Submission returns as soon as the job row
      // exists, so the outbox is still empty at this point - these tests used to
      // read it immediately and were relying on the walk having already happened.
      const snap = await waitForSnapshot(ws.workspaceId, job.body.jobId);

      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 LIMIT 2',
        [job.body.jobId],
      );
      for (const r of rows) {
        await recordJobTransition(
          ws.workspaceId,
          job.body.jobId,
          r.opportunity_id,
          null,
          newLead(),
        );
      }

      const res = await api<{
        status: string;
        totalMatched: number;
        snapshotAt: string;
        batches: Record<string, number>;
      }>(BASE.transition, `/bulk-moves/${job.body.jobId}`, { workspaceId: ws.workspaceId });

      expect(res.status).toBe(200);
      // The same figure the wait above settled on, so this compares the endpoint
      // against the built batches rather than against a moving target.
      expect(res.body.totalMatched).toBe(snap.totalMatched);
      expect(Number.isNaN(Date.parse(res.body.snapshotAt))).toBe(false);

      // The counts are batches, not records: there is no per-record state left to
      // report. total_matched is the record count and is authoritative, because it
      // is the size of what was written into the batches.
      //
      // Deliberately not asserting that anything is still pending. The live worker
      // is running against this job, and on a fixture this small it can drain the
      // one batch before the status call lands, which made the assertion a race -
      // it passed twice and failed once out of three runs. What is stable is that
      // every batch is accounted for in exactly one state.
      const total = Object.values(res.body.batches).reduce((a, b) => a + b, 0);
      const expectedBatches = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
        [job.body.jobId],
      );
      expect(total).toBe(expectedBatches.rows[0]!.n);
    });

    it('returns 404 for an unknown id', async () => {
      const res = await api<{ message: string }>(BASE.transition, `/bulk-moves/${randomUUID()}`, {
        workspaceId: ws.workspaceId,
      });
      expect(res.status).toBe(404);
    });

    it('rejects a malformed id with 400', async () => {
      const res = await api<{ message: string }>(BASE.transition, '/bulk-moves/nope', {
        workspaceId: ws.workspaceId,
      });
      expect(res.status).toBe(400);
    });
  });

  describe('GET /bulk-moves/:id/transitions', () => {
    it('is empty before any work has run', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      // Waited for, not because the assertion needs the batches, but because
      // "before any work has run" is only true if the build is finished. The live
      // worker can start moving this job while the test is still setting up, and
      // then the endpoint is legitimately non-empty and the test fails for a
      // reason that has nothing to do with pagination.
      await waitForJobSettled(ws.workspaceId, job.body.jobId, 30_000);
      const res = await api<{ items: unknown[]; nextCursor: string | null }>(
        BASE.transition,
        `/bulk-moves/${job.body.jobId}/transitions`,
        { workspaceId: ws.workspaceId },
      );
      expect(res.status).toBe(200);
      expect(res.body.items).toEqual([]);
      expect(res.body.nextCursor).toBeNull();
    });

    it('returns stage names and outcomes, not bare ids', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      // The batch has to exist before its ids can be read - see the note in the
      // status test above.
      await waitForSnapshot(ws.workspaceId, job.body.jobId);
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 LIMIT 1',
        [job.body.jobId],
      );
      await recordJobTransition(
        ws.workspaceId,
        job.body.jobId,
        rows[0]!.opportunity_id,
        ws.stages['contacted'],
        newLead(),
      );

      const res = await api<{
        items: { to_stage: string; to_stage_name: string; to_outcome: string }[];
      }>(BASE.transition, `/bulk-moves/${job.body.jobId}/transitions`, {
        workspaceId: ws.workspaceId,
      });
      expect(res.body.items).toHaveLength(1);
      expect(res.body.items[0]!.to_stage).toBe(newLead());
      expect(res.body.items[0]!.to_outcome).toBe('open');
      // the name is what makes the output readable; a bare uuid does not
      const { rows: nameRows } = await pool.query<{ name: string }>(
        'SELECT name FROM stage WHERE id = $1',
        [newLead()],
      );
      expect(res.body.items[0]!.to_stage_name).toBe(nameRows[0]!.name);
    });

    it('paginates without repeating a row when timestamps share a millisecond', async () => {
      const job = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      await waitForSnapshot(ws.workspaceId, job.body.jobId);
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 ORDER BY opportunity_id LIMIT 5',
        [job.body.jobId],
      );
      // Sub-millisecond spacing, which a JS Date cannot represent.
      for (const [i, r] of rows.entries()) {
        await insertTransitionWithMicros(
          ws.workspaceId,
          r.opportunity_id,
          newLead(),
          i * 137,
          job.body.jobId,
        );
      }

      const seen: string[] = [];
      let cursor: string | null = null;
      for (let page = 0; page < 10; page++) {
        const suffix: string = cursor === null ? '' : `&cursor=${cursor}`;
        const url: string = `/bulk-moves/${job.body.jobId}/transitions?limit=2${suffix}`;
        const res: ApiResponseLike<{ items: { id: string }[]; nextCursor: string | null }> =
          await api(BASE.transition, url, { workspaceId: ws.workspaceId });
        expect(res.status).toBe(200);
        seen.push(...res.body.items.map((i) => i.id));
        cursor = res.body.nextCursor;
        if (cursor === null) break;
      }

      expect(seen).toHaveLength(rows.length);
      expect(new Set(seen).size).toBe(rows.length);
    });

    it('rejects a cursor that belongs to a different job', async () => {
      const mine = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      const theirs = await submitBulkMove(ws.workspaceId, { targetStageId: newLead() });
      await waitForSnapshot(ws.workspaceId, theirs.body.jobId);
      const { rows } = await pool.query<{ opportunity_id: string }>(
        'SELECT unnest(item_ids) AS opportunity_id FROM bulk_job_outbox WHERE job_id = $1 LIMIT 1',
        [theirs.body.jobId],
      );
      const theirsTransition = await insertTransitionWithMicros(
        ws.workspaceId,
        rows[0]!.opportunity_id,
        newLead(),
        0,
      );
      await pool.query('UPDATE opportunity_transition SET job_id = $1 WHERE id = $2', [
        theirs.body.jobId,
        theirsTransition,
      ]);

      const res = await api<{ message: string }>(
        BASE.transition,
        `/bulk-moves/${mine.body.jobId}/transitions?cursor=${theirsTransition}`,
        { workspaceId: ws.workspaceId },
      );
      expect(res.status).toBe(400);
      expect(res.body.message).toMatch(/cursor/);
    });
  });
});
