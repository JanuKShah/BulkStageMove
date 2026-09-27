import { randomUUID } from 'node:crypto';
import {
  api,
  pool,
  provisionWorkspace,
  waitForSnapshot,
  withDeadlockRetry,
  type TestWorkspace,
} from '../helpers';

/**
 * Filters on a bulk move, as opposed to on the opportunity list.
 *
 * `filtering.spec.ts` covers GET /opportunities thoroughly, and that is a
 * different code path: the list reads a page at a time through the opportunity
 * service, while a bulk move's filter is stored on the job row and replayed by
 * SnapshotBuilder through the predicate in shared/filter/snapshot-query.ts. A
 * filter that is correct in one and wrong in the other would pass the list suite
 * and move the wrong records, so these go through the submit path.
 *
 * Every count is checked against a direct SQL count of the same predicate, and
 * the ids that ended up in the batches are checked too - a right count over the
 * wrong rows is the failure that matters here.
 *
 * The regression test at the end is the one that matters most. A filter that
 * names stages and resolves to none used to be stored as `{}`, and a job with no
 * stage filter matches the entire workspace: asking to move the lost deals moved
 * everything instead. It is here because it is the shape of bug this suite exists
 * to catch, not because it is an edge case.
 */
describe('a bulk move filters on status, date and value', () => {
  let ws: TestWorkspace;
  const open = () => ws.stages['newLead'];
  const won = () => ws.stages['closedWon'];

  beforeAll(async () => {
    ws = await provisionWorkspace('bulkfilter');
  });

  afterAll(async () => {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await pool.end();
  });

  const stage = async (outcome: 'open' | 'won' | 'lost' | 'abandoned'): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO stage (workspace_id, name, outcome)
       VALUES ($1, $2, $3) RETURNING id`,
      [ws.workspaceId, `bf-${outcome}-${randomUUID().slice(0, 6)}`, outcome],
    );
    return rows[0]!.id;
  };

  /** Records with a chosen stage, value and creation date, so each filter has something to select. */
  const seed = async (
    stageId: string,
    count: number,
    opts: { from?: number; step?: number; day?: number } = {},
  ): Promise<string[]> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value, created_at)
       SELECT $1, $2, 'bf-' || g,
              $3 + g * $4,
              timestamptz '2026-03-01 00:00:00Z' + ($5 + g) * interval '1 day'
         FROM generate_series(1, $6::int) g
       RETURNING id`,
      [
        ws.workspaceId,
        stageId,
        opts.from ?? 1000,
        opts.step ?? 100,
        opts.day ?? 0,
        count,
      ],
    );
    return rows.map((r) => r.id);
  };

  const submit = async (body: Record<string, unknown>): Promise<string> => {
    const res = await api<{ jobId: string }>(
      'http://localhost:3005',
      '/bulk-moves',
      {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ idempotencyKey: `bf-${randomUUID()}`, ...body }),
      },
    );
    expect(res.status).toBe(201);
    return res.body.jobId;
  };

  /** The ids the walk actually put into batches, which is the set that gets moved. */
  const matched = async (jobId: string): Promise<string[]> => {
    const { rows } = await pool.query<{ id: string }>(
      'SELECT unnest(item_ids) AS id FROM bulk_job_outbox WHERE job_id = $1',
      [jobId],
    );
    return rows.map((r) => r.id);
  };

  const sqlIds = async (where: string): Promise<string[]> => {
    const { rows } = await pool.query<{ id: string }>(
      `SELECT id FROM opportunity
        WHERE workspace_id = $1 AND ${where}
        ORDER BY created_at, id`,
      [ws.workspaceId],
    );
    return rows.map((r) => r.id);
  };

  it('a status filter moves only the records in that outcome', async () => {
    // 'lost' is created here specifically so the outcome resolves to a real
    // stage. provisionWorkspace only makes open and won.
    const lostStage = await stage('lost');
    const mine = await seed(lostStage, 6, { from: 5000, step: 100 });
    // Decoys in the same workspace that the filter must exclude.
    await seed(open(), 9, { from: 7000, step: 100 });
    await seed(won(), 7, { from: 9000, step: 100 });

    const jobId = await submit({ outcome: 'lost', targetStageId: won() });
    const snap = await waitForSnapshot(ws.workspaceId, jobId);

    const expected = await sqlIds(`stage_id = '${lostStage}'`);
    expect(snap.totalMatched).toBe(expected.length);
    expect(snap.totalMatched).toBe(6);
    // The set, not just the size. A right count over the wrong rows is the
    // failure worth catching, and the decoys are sized differently from the
    // answer on purpose so a mix-up cannot produce the same number.
    expect(new Set(await matched(jobId))).toEqual(new Set(expected));
    expect(mine).toHaveLength(6);
  });

  it('a date range filter moves only the records created in that window', async () => {
    const src = await stage('open');
    await seed(src, 30, { day: 0 }); // 2026-03-02 .. 2026-03-31
    // Outside the window entirely, and a decoy inside the workspace whose value
    // would match a value filter but whose date must not.
    await seed(src, 5, { day: 200 });

    const jobId = await submit({
      stageId: [src],
      createdFrom: '2026-03-10T00:00:00Z',
      createdTo: '2026-03-15T00:00:00Z',
      targetStageId: won(),
    });
    const snap = await waitForSnapshot(ws.workspaceId, jobId);

    const expected = await sqlIds(
      `stage_id = '${src}' AND created_at >= '2026-03-10T00:00:00Z' AND created_at <= '2026-03-15T00:00:00Z'`,
    );
    expect(snap.totalMatched).toBe(expected.length);
    expect(snap.totalMatched).toBeGreaterThan(0);
    expect(snap.totalMatched).toBeLessThan(35);
    expect(new Set(await matched(jobId))).toEqual(new Set(expected));

    // Inclusive on both ends, and the window is a real subset of what exists.
    for (const id of await matched(jobId)) {
      const { rows } = await pool.query<{ created_at: Date }>(
        'SELECT created_at FROM opportunity WHERE id = $1',
        [id],
      );
      const t = rows[0]!.created_at.getTime();
      expect(t).toBeGreaterThanOrEqual(Date.parse('2026-03-10T00:00:00Z'));
      expect(t).toBeLessThanOrEqual(Date.parse('2026-03-15T00:00:00Z'));
    }
  });

  it('a value range filter moves only the records in that band', async () => {
    const src = await stage('open');
    await seed(src, 20, { from: 1000, step: 100 }); // 1100 .. 3000
    await seed(src, 20, { from: 90_000, step: 500 }); // 90,500 .. 99,500

    const jobId = await submit({
      stageId: [src],
      minValue: 50_000,
      maxValue: 95_000,
      targetStageId: won(),
    });
    const snap = await waitForSnapshot(ws.workspaceId, jobId);

    const expected = await sqlIds(
      `stage_id = '${src}' AND value >= 50000 AND value <= 95000`,
    );
    expect(snap.totalMatched).toBe(expected.length);
    expect(snap.totalMatched).toBeGreaterThan(0);
    expect(snap.totalMatched).toBeLessThan(40);
    expect(new Set(await matched(jobId))).toEqual(new Set(expected));

    // Both bounds, checked on the rows rather than the count: a filter that
    // ignored maxValue would still produce a plausible number.
    for (const id of await matched(jobId)) {
      const { rows } = await pool.query<{ value: string }>(
        'SELECT value FROM opportunity WHERE id = $1',
        [id],
      );
      const v = Number(rows[0]!.value);
      expect(v).toBeGreaterThanOrEqual(50_000);
      expect(v).toBeLessThanOrEqual(95_000);
    }
  });

  it('combines the three, and matches the same predicate in SQL', async () => {
    const lostStage = await stage('lost');
    await seed(lostStage, 15, { from: 60_000, step: 200, day: 0 });
    await seed(lostStage, 15, { from: 60_000, step: 200, day: 100 });
    // A different outcome, not a second lost stage. An outcome resolves to every
    // stage carrying it, so a second 'lost' stage would be in scope and the
    // decoy would be testing the wrong thing.
    const abandoned = await stage('abandoned');
    await seed(abandoned, 15, { from: 60_000, step: 200, day: 0 });

    const jobId = await submit({
      outcome: 'lost',
      minValue: 60_000,
      maxValue: 65_000,
      createdFrom: '2026-03-05T00:00:00Z',
      createdTo: '2026-03-20T00:00:00Z',
      targetStageId: won(),
    });
    const snap = await waitForSnapshot(ws.workspaceId, jobId);

    const expected = await sqlIds(
      `stage_id = '${lostStage}'
         AND value >= 60000 AND value <= 65000
         AND created_at >= '2026-03-05T00:00:00Z'
         AND created_at <= '2026-03-20T00:00:00Z'`,
    );
    expect(snap.totalMatched).toBe(expected.length);
    expect(snap.totalMatched).toBeGreaterThan(0);
    expect(new Set(await matched(jobId))).toEqual(new Set(expected));
  });

  it('a filter naming an outcome with no stage matches NOTHING', async () => {
    // The regression. A workspace with no 'abandoned' stage resolves that outcome
    // to an empty list, and the empty list used to be dropped - leaving a filter of
    // `{}`, which matches every record in the workspace. Asking to move the
    // abandoned deals therefore moved all of them, or would have with a permitted
    // transition in place.
    //
    // Its own workspace, because the property is about a workspace that lacks the
    // outcome, and the tests above create abandoned stages of their own. Sharing
    // one would make this test pass or fail on the order the file happened to run.
    const fresh = await provisionWorkspace('no-abandoned');
    try {
      // One stage, picked as a scalar subquery. Joining generate_series against
      // the whole stage table would cross-product into three times the rows.
      const { rows: seeded } = await pool.query<{ id: string }>(
        `INSERT INTO opportunity (workspace_id, stage_id, name, value)
         SELECT $1, s.id, 'na-' || g, 100
           FROM generate_series(1, 12) g,
                (SELECT id FROM stage WHERE workspace_id = $1 ORDER BY created_at LIMIT 1) s
         RETURNING id`,
        [fresh.workspaceId],
      );
      expect(seeded).toHaveLength(12);

      const noAbandoned = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM stage
          WHERE workspace_id = $1 AND outcome = 'abandoned'`,
        [fresh.workspaceId],
      );
      expect(noAbandoned.rows[0]!.n).toBe(0);

      const res = await api<{ jobId: string }>('http://localhost:3005', '/bulk-moves', {
        method: 'POST',
        workspaceId: fresh.workspaceId,
        body: JSON.stringify({
          idempotencyKey: `na-${randomUUID()}`,
          outcome: 'abandoned',
          targetStageId: fresh.stages['closedWon'],
        }),
      });
      expect(res.status).toBe(201);

      const snap = await waitForSnapshot(fresh.workspaceId, res.body.jobId);
      expect(snap.totalMatched).toBe(0);
      expect(snap.status).toBe('completed');

      // And nothing moved. A count of zero would also be reported by a fix that
      // wrote batches and then claimed none, so the records themselves are checked:
      // all twelve were seeded in newLead and none should have left it.
      const { rows: left } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM opportunity
          WHERE workspace_id = $1 AND stage_id <> $2`,
        [fresh.workspaceId, fresh.stages['newLead']],
      );
      expect(left[0]!.n).toBe(0);
      const { rows: stillThere } = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1 AND stage_id = $2',
        [fresh.workspaceId, fresh.stages['newLead']],
      );
      expect(stillThere[0]!.n).toBe(12);
    } finally {
      await withDeadlockRetry(() =>
        pool.query('DELETE FROM workspace WHERE id = $1', [fresh.workspaceId]),
      );
    }
  });

  it('an explicitly empty stage list is refused, not read as no filter', async () => {
    // The same hazard by the other door. A body carrying {"stageId": []} - a
    // client with nothing selected - used to parse as absent, and a job with no
    // stage filter moves the whole workspace. Refused with a 400, the same way an
    // explicit null already is.
    const before = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1',
      [ws.workspaceId],
    );
    const res = await api<{ message: string }>('http://localhost:3005', '/bulk-moves', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({
        idempotencyKey: `bf-${randomUUID()}`,
        stageId: [],
        targetStageId: won(),
      }),
    });
    expect(res.status).toBe(400);
    expect(res.body.message).toMatch(/stageId/);

    const after = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM opportunity WHERE workspace_id = $1',
      [ws.workspaceId],
    );
    expect(after.rows[0]!.n).toBe(before.rows[0]!.n);
  });

  it('a filter that names no stages at all still matches the whole workspace', async () => {
    // The control, and the reason the fix is `!== undefined` rather than always
    // adding the clause: "no filter" and "a filter that matched nothing" have to
    // stay distinguishable, and the first is a legitimate bulk move.
    const jobId = await submit({ targetStageId: won() });
    const snap = await waitForSnapshot(ws.workspaceId, jobId);
    const all = await sqlIds('1=1');
    expect(snap.totalMatched).toBe(all.length);
    expect(snap.totalMatched).toBeGreaterThan(0);
    expect(new Set(await matched(jobId))).toEqual(new Set(all));
  });
});
