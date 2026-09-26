import { randomUUID } from 'node:crypto';
import {
  BASE,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  seedOpportunities,
  type TestWorkspace,
} from '../helpers';

/**
 * What the idempotency key does and does not guarantee under concurrency.
 *
 * HOLDS: one key creates exactly one job, enforced by the constraint rather than
 * by application logic, so no number of racers can double-apply.
 *
 * DOES NOT HOLD: the response to the callers that lose. The pre-check cannot see
 * them, so they surface the raw constraint violation as a 500. Deferred to a
 * distributed lock.
 *
 * There is deliberately no status-code test for those 500s. The race fires 92%,
 * 67% and 83% of the time for 5, 10 and 20 callers, because node's fetch pools
 * about six connections per origin and requests queue rather than overlap. A
 * test that fails one build in ten is worse than none. Add the assertions here
 * when the lock lands.
 */
describe('concurrent submissions on one idempotency key', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('race');
    await seedOpportunities(ws.workspaceId, ws.stages['newLead'], 20);
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  const submit = (key: string): Promise<number> => {
    const body = JSON.stringify({ idempotencyKey: key, targetStageId: ws.stages['contacted'] });
    return fetch(`${BASE.transition}/bulk-moves`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-workspace-id': ws.workspaceId },
      body,
    }).then((r) => r.status);
  };

  it('never creates a second job for one key, however many callers race', async () => {
    for (let round = 0; round < 4; round++) {
      const key = randomUUID();
      await Promise.all(Array.from({ length: 8 }, () => submit(key)));
      const { rows } = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM bulk_job WHERE idempotency_key = $1',
        [key],
      );
      expect(rows[0]!.n).toBe(1);
    }
  });

  it('leaves no half-written job behind when callers lose the race', async () => {
    // The losers must not have created items against a job that then failed, and
    // a job that did win must still be internally consistent.
    const key = randomUUID();
    await Promise.all(Array.from({ length: 8 }, () => submit(key)));
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM bulk_job j
       WHERE j.idempotency_key = $1
         AND j.total_matched <> (SELECT count(*) FROM bulk_job_item i WHERE i.job_id = j.id)`,
      [key],
    );
    expect(rows[0]!.n).toBe(0);
  });

  it('a losing caller cannot have written a second job in another workspace', async () => {
    // The key is unique per workspace, so the same key in two workspaces is two
    // unrelated submissions and both must be honoured.
    const other = await provisionWorkspace('race-other');
    const key = randomUUID();
    await Promise.all([
      submit(key),
      fetch(`${BASE.transition}/bulk-moves`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-workspace-id': other.workspaceId },
        body: JSON.stringify({ idempotencyKey: key, targetStageId: other.stages['contacted'] }),
      }),
    ]);
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(DISTINCT workspace_id)::int AS n FROM bulk_job WHERE idempotency_key = $1',
      [key],
    );
    expect(rows[0]!.n).toBe(2);
    await destroyWorkspace(other.workspaceId);
  });
});
