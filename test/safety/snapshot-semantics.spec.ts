import { randomUUID } from 'node:crypto';
import {
  api,
  createJobWithItems,
  pool,
  provisionWorkspace,
  resetBatchTo,
  seedOpportunities,
  seedOpportunitiesInStage,
  submitBulkMove,
  waitForSnapshot,
  withDeadlockRetry,
  type TestWorkspace,
} from '../helpers';
import { closeWorkerHarness, processBatchForTest } from './worker-harness';

/**
 * The behaviour that replaced a materialised snapshot.
 *
 * A bulk job used to write one row per opportunity. It now writes one row per
 * batch, holding the ids, plus a watermark on the job. Everything asserted here
 * is a consequence of that: what the watermark does, what happens to a record
 * that stops matching, and what the status response can still tell a caller.
 */
describe('batches hold records instead of rows', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('snapshot');
    await seedOpportunities(ws.workspaceId, ws.stages['contacted'], 30);
  });

  afterAll(async () => {
    await closeWorkerHarness();
    await withDeadlockRetry(() => pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]));
    await pool.end();
  });

  const contacted = () => ws.stages['contacted'];
  const closedWon = () => ws.stages['closedWon'];
  const newLead = () => ws.stages['newLead'];

  it('writes one row per batch, not one row per record', async () => {
    await seedOpportunitiesInStage(ws.workspaceId, contacted(), 2_500);
    const res = await submitBulkMove(ws.workspaceId, {
      targetStageId: closedWon(),
      stageId: contacted(),
    });
    expect(res.status).toBe(201);
    const snap = await waitForSnapshot(ws.workspaceId, res.body.jobId);
    expect(snap.totalMatched).toBe(2_530);

    const { rows } = await pool.query<{ batches: number; records: number }>(
      `SELECT count(*)::int AS batches, sum(cardinality(item_ids))::int AS records
         FROM bulk_job_outbox WHERE job_id = $1`,
      [res.body.jobId],
    );
    // 2,530 records is three batches. The old shape would have been 2,530 rows.
    expect(rows[0]!.batches).toBe(3);
    expect(rows[0]!.records).toBe(2_530);
  });

  it('gives every record to exactly one batch', async () => {
    const res = await submitBulkMove(ws.workspaceId, {
      targetStageId: closedWon(),
      stageId: contacted(),
    });
    expect(res.status).toBe(201);
    // Two subqueries rather than one over a lateral unnest: joining a set-returning
    // function to the table duplicates the batch rows, so cardinality(item_ids)
    // would be summed once per id it contains - 2,280,900 rather than 2,530.
    const { rows } = await pool.query<{ total: number; distinct_ids: number }>(
      `SELECT (SELECT coalesce(sum(cardinality(item_ids)), 0)::int
                 FROM bulk_job_outbox WHERE job_id = $1) AS total,
              (SELECT count(DISTINCT opportunity_id)::int
                 FROM bulk_job_outbox, unnest(item_ids) AS u(opportunity_id)
                WHERE job_id = $1) AS distinct_ids`,
      [res.body.jobId],
    );
    // A record in two batches would be moved twice and counted twice.
    expect(rows[0]!.total).toBe(rows[0]!.distinct_ids);
    // And the count agrees with what the job reports, which is the number that
    // actually landed in batches rather than one taken before the walk.
    const snap = await waitForSnapshot(ws.workspaceId, res.body.jobId);
    expect(rows[0]!.total).toBe(snap.totalMatched);
  });

  it('excludes an opportunity created after the watermark', async () => {
    const before = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 10);
    const res = await submitBulkMove(ws.workspaceId, {
      targetStageId: closedWon(),
      stageId: contacted(),
    });
    expect(res.status).toBe(201);

    // Created after submission, so it must never appear in this job's batches even
    // though it matches the filter. This is the guarantee the brief asks for, and
    // it is what the watermark is for.
    const after = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 5);
    expect(after).toHaveLength(5);

    const { rows } = await pool.query<{ swept: number }>(
      `SELECT count(*)::int AS swept
         FROM bulk_job_outbox b, unnest(b.item_ids) AS t(opportunity_id)
        WHERE b.job_id = $1 AND t.opportunity_id = ANY($2::uuid[])`,
      [res.body.jobId, after],
    );
    expect(rows[0]!.swept).toBe(0);

    const all = await pool.query<{ swept: number }>(
      `SELECT count(*)::int AS swept
         FROM bulk_job_outbox b, unnest(b.item_ids) AS t(opportunity_id)
        WHERE b.job_id = $1 AND t.opportunity_id = ANY($2::uuid[])`,
      [res.body.jobId, before],
    );
    expect(all.rows[0]!.swept).toBe(before.length);
  });

  it('skips a record that leaves the filtered stage, and says so', async () => {
    const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 4);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });
    // Put the job's filter on the stage these records are in, so leaving it is
    // detectable. Without this the filter names no stages and nothing is out of
    // scope, which would test nothing.
    await pool.query('UPDATE bulk_job SET filter = $2 WHERE id = $1', [
      jobId,
      JSON.stringify({ stageId: [contacted()] }),
    ]);

    // A user moves one of them out of the filtered stage before the batch runs.
    const moved = ids[0]!;
    await pool.query('UPDATE opportunity SET stage_id = $2 WHERE id = $1', [moved, newLead()]);

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    // Three move, the fourth is recognised as out of scope. It is neither moved
    // nor reported as a failure, because undoing a deliberate change is worse than
    // not finishing the job.
    expect(result.moved).toBe(3);
    expect(result.failed).toBe(0);
    expect(result.skipped).toBe(1);

    const stage = await pool.query<{ stage_id: string }>(
      'SELECT stage_id FROM opportunity WHERE id = $1',
      [moved],
    );
    // The user's change stands. The job did not stomp it.
    expect(stage.rows[0]!.stage_id).toBe(newLead());

    const failures = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM bulk_job_failure WHERE job_id = $1',
      [jobId],
    );
    expect(failures.rows[0]!.n).toBe(0);
  });

  it('records an unmovable record as a failure, with the reason', async () => {
    // newLead -> closedWon has no rule, so the whole batch is refused.
    const ids = await seedOpportunitiesInStage(ws.workspaceId, newLead(), 3);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(0);
    expect(result.failed).toBe(3);

    const res = await api<unknown[]>(
      'http://localhost:3005',
      `/bulk-moves/${jobId}/failures`,
      { workspaceId: ws.workspaceId },
    );
    expect(res.status).toBe(200);
    // The endpoint answers with the array itself, not an envelope.
    expect(res.body).toHaveLength(3);
    const first = res.body[0] as { error: string };
    expect(first.error).toMatch(/no permitted transition/);
  });

  it('completes a job that matched nothing instead of leaving it pending', async () => {
    const empty = await provisionWorkspace('snapshot-empty');
    try {
      const res = await submitBulkMove(empty.workspaceId, {
        targetStageId: empty.stages['newLead'],
        outcome: 'won',
      });
      expect(res.status).toBe(201);
      expect((await waitForSnapshot(ws.workspaceId, res.body.jobId)).totalMatched).toBe(0);

      // No batches means nothing would ever settle it, so it is finished on
      // creation. A job stuck at pending for ever is the failure mode here.
      const { rows } = await pool.query<{ status: string; completed_at: Date | null }>(
        'SELECT status, completed_at FROM bulk_job WHERE id = $1',
        [res.body.jobId],
      );
      expect(rows[0]!.status).toBe('completed');
      expect(rows[0]!.completed_at).not.toBeNull();

      const batches = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM bulk_job_outbox WHERE job_id = $1',
        [res.body.jobId],
      );
      expect(batches.rows[0]!.n).toBe(0);
    } finally {
      await withDeadlockRetry(() =>
        pool.query('DELETE FROM workspace WHERE id = $1', [empty.workspaceId]),
      );
    }
  });

  it('keeps the job counters equal to what the batches settled', async () => {
    const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 6);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });
    await processBatchForTest(ws.workspaceId, jobId, batchNo);

    const { rows } = await pool.query<{
      total_matched: number;
      processed_count: number;
      failed_count: number;
    }>('SELECT total_matched, processed_count, failed_count FROM bulk_job WHERE id = $1', [jobId]);

    // The counters move by a delta, not a sum: a sum read under the row lock that
    // every batch contends on is stale by whatever committed while it waited, and
    // the last writer's stale value is the one that survives. That reported
    // processed_count of 49,000 on a 50,000 record job.
    expect(rows[0]!.processed_count).toBe(rows[0]!.total_matched);
    expect(rows[0]!.failed_count).toBe(0);
  });

  it('re-running a settled batch changes nothing', async () => {
    const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 4);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });
    await processBatchForTest(ws.workspaceId, jobId, batchNo);

    // A redelivered message finds a completed batch and claims nothing, so the
    // counters are untouched rather than doubled.
    await resetBatchTo(jobId, batchNo, 'pending');
    const again = await processBatchForTest(ws.workspaceId, jobId, batchNo);

    const { rows } = await pool.query<{ processed_count: number; total_matched: number }>(
      'SELECT processed_count, total_matched FROM bulk_job WHERE id = $1',
      [jobId],
    );
    expect(again.moved).toBe(4);
    expect(rows[0]!.processed_count).toBe(rows[0]!.total_matched);
  });

  it('surfaces a batch that exhausted its attempts, and the reason', async () => {
    const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 5);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });

    // Nothing consumes the dead letter queue, deliberately: a batch that has spent
    // its budget should not be resurrected automatically. That makes the batch row
    // the only durable record, so the status response has to carry it - otherwise
    // these records are in neither the per-record failures nor the job counters
    // and are simply not moved.
    await pool.query(
      `UPDATE bulk_job_outbox
          SET status = 'failed', failed_count = cardinality(item_ids), error = $3,
              attempts = 3, completed_at = now(), updated_at = now()
        WHERE job_id = $1 AND batch_no = $2`,
      [jobId, batchNo, 'attempts exhausted'],
    );

    const res = await api<{
      totalMatched: number;
      deadLettered: {
        batches: number;
        records: number;
        reasons: { reason: string; batches: number }[];
      };
    }>('http://localhost:3005', `/bulk-moves/${jobId}`, { workspaceId: ws.workspaceId });

    expect(res.status).toBe(200);
    expect(res.body.deadLettered.batches).toBe(1);
    expect(res.body.deadLettered.records).toBe(5);
    expect(res.body.deadLettered.reasons).toEqual([
      { reason: 'attempts exhausted', batches: 1 },
    ]);
    // These five records are unmoved, and totalMatched is the honest figure they
    // are measured against - it is not quietly reduced to hide the loss.
    expect(res.body.totalMatched).toBe(5);
  });

  it('does not call a partly-applied batch dead-lettered', async () => {
    // The distinction that matters: 990 records moved and 10 could not be
    // transitioned is a business answer the user acts on, reported per record by
    // `failures`. It is not a batch that ran out of attempts, and conflating the
    // two would page an operator for something nobody needs to fix.
    //
    // Only failBatch writes status='failed', and it is reached solely when the
    // attempt budget is spent. A batch with unmovable records goes through
    // settleBatch and lands on 'completed' with failed_count set.
    const movable = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 6);
    const stuck = await seedOpportunitiesInStage(ws.workspaceId, newLead(), 3);
    const { jobId, batchNo } = await createJobWithItems(
      ws.workspaceId,
      closedWon(),
      [...movable, ...stuck],
      { enqueue: false },
    );

    const result = await processBatchForTest(ws.workspaceId, jobId, batchNo);
    expect(result.moved).toBe(6);
    expect(result.failed).toBe(3);

    const res = await api<{
      deadLettered: { batches: number; records: number };
      failedCount: number;
    }>('http://localhost:3005', `/bulk-moves/${jobId}`, { workspaceId: ws.workspaceId });

    expect(res.status).toBe(200);
    expect(res.body.deadLettered.batches).toBe(0);
    expect(res.body.deadLettered.records).toBe(0);
    // The three are reported as failures instead, which is the actionable list.
    expect(res.body.failedCount).toBe(3);

    const batch = await pool.query<{ status: string; failed_count: number }>(
      'SELECT status, failed_count FROM bulk_job_outbox WHERE job_id = $1 AND batch_no = $2',
      [jobId, batchNo],
    );
    expect(batch.rows[0]!.status).toBe('completed');
    expect(batch.rows[0]!.failed_count).toBe(3);
  });

  it('reports no dead-lettered batches for a healthy job', async () => {
    const ids = await seedOpportunitiesInStage(ws.workspaceId, contacted(), 4);
    const { jobId, batchNo } = await createJobWithItems(ws.workspaceId, closedWon(), ids, {
      enqueue: false,
    });
    await processBatchForTest(ws.workspaceId, jobId, batchNo);

    const res = await api<{ deadLettered: { batches: number; reasons: unknown[] } }>(
      'http://localhost:3005',
      `/bulk-moves/${jobId}`,
      { workspaceId: ws.workspaceId },
    );
    expect(res.status).toBe(200);
    // Zero is the value worth asserting: it is what makes a non-zero alertable.
    expect(res.body.deadLettered.batches).toBe(0);
    expect(res.body.deadLettered.reasons).toEqual([]);
  });

  it('reports the watermark the job was submitted at', async () => {
    const res = await submitBulkMove(ws.workspaceId, {
      targetStageId: closedWon(),
      stageId: contacted(),
    });
    expect(res.status).toBe(201);
    const body = await api<{
      snapshotAt: string;
      totalMatched: number;
      batches: Record<string, number>;
    }>('http://localhost:3005', `/bulk-moves/${res.body.jobId}`, {
      workspaceId: ws.workspaceId,
    });

    expect(body.status).toBe(200);
    expect(Number.isNaN(Date.parse(body.body.snapshotAt))).toBe(false);
    // total_matched is the size of what was written into the batches, so a caller
    // can reconcile the two without trusting a counter.
    const batches = await pool.query<{ records: number }>(
      'SELECT coalesce(sum(cardinality(item_ids)), 0)::int AS records FROM bulk_job_outbox WHERE job_id = $1',
      [res.body.jobId],
    );
    expect(body.body.totalMatched).toBe(batches.rows[0]!.records);
    expect(Object.values(body.body.batches).reduce((a, b) => a + b, 0)).toBeGreaterThan(0);
  });

  it('rejects a replay whose filter differs, as before', async () => {
    const key = randomUUID();
    const first = await submitBulkMove(ws.workspaceId, { targetStageId: closedWon() }, key);
    expect(first.status).toBe(201);

    const same = await submitBulkMove(ws.workspaceId, { targetStageId: closedWon() }, key);
    expect(same.status).toBe(201);
    expect(same.body.replay).toBe(true);
    expect(same.body.jobId).toBe(first.body.jobId);

    const different = await submitBulkMove(
      ws.workspaceId,
      { targetStageId: newLead() },
      key,
    );
    expect(different.status).toBe(409);
  });
});
