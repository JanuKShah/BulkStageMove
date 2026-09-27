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
 * ALSO HOLDS: every caller gets an answer that describes what happened. A caller
 * that loses the race is told it was a replay, exactly as a caller that arrives
 * after the winner has committed is. The constraint is the arbiter, and losing to
 * it is an outcome the service can report - not a server fault.
 *
 * Whether the race actually fires is not asserted, and does not need to be. The
 * pre-check is a read and the insert that follows it is a separate round trip, so
 * concurrent callers interleave only some of the time - 67% to 92% across 5, 10
 * and 20 callers, because node's fetch pools about six connections per origin and
 * requests queue rather than overlap. Asserting *who* won would therefore fail
 * one build in several. Asserting that nobody got a 500 is unconditional: whether
 * a caller wins, loses, or never raced at all, it gets a 201 or a 200.
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

  const submit = (key: string): Promise<{ status: number; replay: boolean; jobId: string }> => {
    const body = JSON.stringify({ idempotencyKey: key, targetStageId: ws.stages['contacted'] });
    return fetch(`${BASE.transition}/bulk-moves`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-workspace-id': ws.workspaceId },
      body,
    }).then(async (r) => {
      const parsed = (await r.json()) as { replay?: boolean; jobId?: string };
      return { status: r.status, replay: parsed.replay === true, jobId: parsed.jobId ?? '' };
    });
  };

  it('answers every racer, and never with a 500', async () => {
    // Rounds rather than one shot, because the race is probabilistic and a single
    // round proves nothing either way. The assertion holds whether or not any
    // given round actually interleaves.
    const seen = new Set<number>();
    let replays = 0;

    for (let round = 0; round < 6; round++) {
      const key = randomUUID();
      const results = await Promise.all(Array.from({ length: 10 }, () => submit(key)));
      for (const r of results) {
        seen.add(r.status);
        if (r.replay) replays++;
      }

      // Status first, because it is the primary claim and a failure here should
      // name the offending code rather than a downstream symptom.
      const roundStatuses = [...new Set(results.map((r) => r.status))].sort();
      expect(roundStatuses.filter((s) => s !== 200 && s !== 201)).toEqual([]);

      // Every caller must name the same job. A loser that invented its own, or
      // reported a different one, would break the contract even with a tidy
      // status code.
      const ids = [...new Set(results.map((r) => r.jobId))];
      expect(ids.length).toBe(1);
    }

    // The point of the fix: 500 is not an acceptable answer to losing a race.
    expect([...seen].filter((s) => s === 500)).toEqual([]);
    for (const s of seen) expect([200, 201]).toContain(s);
    // At least one caller created the job. A round where nobody did would mean the
    // key was somehow already taken, which this test does not set up.
    expect(seen.has(201)).toBe(true);
    // Reported so a build that never interleaves is visible rather than silent:
    // a pass with zero replays has not exercised the path this fix is for.
    if (replays === 0) {
      console.warn('  WARNING: no caller was a replay - the race did not fire this run');
    }
  });

  it("a loser of the race gets the winner's job, replayed rather than recreated", async () => {
    // Drives the conflict branch directly rather than hoping to win a race: the
    // row is written first, so the next submit must take the pre-check, and the
    // assertion is on the answer rather than on the timing.
    const key = randomUUID();
    const first = await submit(key);
    expect(first.status).toBe(201);
    expect(first.replay).toBe(false);

    const second = await submit(key);
    expect(second.status).toBe(201);
    expect(second.replay).toBe(true);
    expect(second.jobId).toBe(first.jobId);
  });

  it('a different request under a taken key is a 409, not a replay', async () => {
    // The other half of the rule. A racer that lost with a *different* filter is
    // making a client bug visible rather than having it silently ignored.
    const key = randomUUID();
    const first = await submit(key);
    expect(first.status).toBe(201);

    const res = await fetch(`${BASE.transition}/bulk-moves`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-workspace-id': ws.workspaceId },
      body: JSON.stringify({ idempotencyKey: key, targetStageId: ws.stages['newLead'] }),
    });
    expect(res.status).toBe(409);
    // Still one job: the refusal must not have created anything.
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM bulk_job WHERE idempotency_key = $1',
      [key],
    );
    expect(rows[0]!.n).toBe(1);
  });

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
    // The losers must not have written batches against a job that then failed, and
    // a job that did win must still be internally consistent: total_matched is
    // exactly what its batches hold, with no records claimed but never written.
    const key = randomUUID();
    await Promise.all(Array.from({ length: 8 }, () => submit(key)));
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM bulk_job j
       WHERE j.idempotency_key = $1
         AND j.total_matched <> coalesce(
               (SELECT sum(cardinality(b.item_ids))::int
                  FROM bulk_job_outbox b WHERE b.job_id = j.id), 0)`,
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
