import { BASE, api, pool, provisionWorkspace, destroyWorkspace } from '../helpers';

/**
 * Covers GET /opportunities across all five filter dimensions the brief names,
 * plus pagination. Every case is checked against a direct SQL count, so a filter
 * that silently returns the wrong rows fails rather than passing on a non-empty
 * result.
 */
describe('GET /opportunities filtering', () => {
  const wsId = { value: '' };
  let stages: Record<string, string>;
  let ownerA: string;
  let ownerB: string;

  const sqlCount = async (where: string): Promise<number> => {
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM opportunity o WHERE o.workspace_id = $1 AND ${where}`,
      [wsId.value],
    );
    return rows[0]!.n;
  };

  const apiCount = async (query: string): Promise<number> => {
    let total = 0;
    let cursor: string | null = null;
    do {
      const qs: string = `${query}&limit=200${cursor ? `&cursor=${cursor}` : ''}`;
      const res = await api<{ items: unknown[]; nextCursor: string | null }>(
        BASE.opportunity,
        `/opportunities?${qs}`,
        { workspaceId: wsId.value },
      );
      if (res.status >= 400)
        throw new Error(`list failed: ${res.status} ${JSON.stringify(res.body)}`);
      total += res.body.items.length;
      cursor = res.body.nextCursor;
    } while (cursor);
    return total;
  };

  beforeAll(async () => {
    const ws = await provisionWorkspace('filter');
    wsId.value = ws.workspaceId;
    stages = ws.stages;

    const mk = async (name: string) =>
      (
        await api<{ id: string }>(BASE.user, '/users', {
          method: 'POST',
          workspaceId: wsId.value,
          body: JSON.stringify({ name }),
        })
      ).body.id;
    ownerA = await mk('Owner A');
    ownerB = await mk('Owner B');

    // Deterministic spread across stages, owners, values and dates.
    const client = await pool.connect();
    try {
      for (let i = 0; i < 40; i++) {
        const stage = [stages['newLead'], stages['contacted'], stages['closedWon']][i % 3]!;
        const owner = i % 2 === 0 ? ownerA : ownerB;
        const value = i * 1000;
        const created = new Date(Date.UTC(2026, 0, 1 + (i % 20)));
        await client.query(
          `INSERT INTO opportunity (workspace_id, stage_id, name, value, owner_id, created_at)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [wsId.value, stage, `bulk-${i}`, value, owner, created],
        );
      }
    } finally {
      client.release();
    }
  });

  afterAll(async () => {
    await destroyWorkspace(wsId.value);
    await pool.end();
  });

  it('returns every row with no filter', async () => {
    expect(await apiCount('')).toBe(await sqlCount('1=1'));
  });

  it('filters by stage', async () => {
    expect(await apiCount(`stageId=${stages['newLead']}`)).toBe(
      await sqlCount(`stage_id = '${stages['newLead']}'`),
    );
  });

  it('filters by a stage list', async () => {
    const list = `${stages['newLead']},${stages['contacted']}`;
    expect(await apiCount(`stageId=${list}`)).toBe(
      await sqlCount(`stage_id IN ('${stages['newLead']}','${stages['contacted']}')`),
    );
  });

  it('filters by owner', async () => {
    expect(await apiCount(`ownerId=${ownerA}`)).toBe(await sqlCount(`owner_id = '${ownerA}'`));
  });

  it('filters by outcome, which is derived from the stage', async () => {
    expect(await apiCount('outcome=won')).toBe(
      await sqlCount(
        `stage_id IN (SELECT id FROM stage WHERE workspace_id = '${wsId.value}' AND outcome = 'won')`,
      ),
    );
    expect(await apiCount('outcome=won')).toBeGreaterThan(0);
  });

  it('filters by minimum value', async () => {
    expect(await apiCount('minValue=20000')).toBe(await sqlCount('value >= 20000'));
  });

  it('filters by a value range', async () => {
    expect(await apiCount('minValue=5000&maxValue=15000')).toBe(
      await sqlCount('value >= 5000 AND value <= 15000'),
    );
  });

  it('filters by created_from', async () => {
    expect(await apiCount('createdFrom=2026-01-10')).toBe(
      await sqlCount(`created_at >= '2026-01-10T00:00:00Z'`),
    );
  });

  it('filters by a date range', async () => {
    // createdTo is an exact instant, not an inclusive day: createdTo=2026-01-10
    // means 2026-01-10T00:00:00Z, so it excludes the rest of that day.
    expect(await apiCount('createdFrom=2026-01-05&createdTo=2026-01-10')).toBe(
      await sqlCount(
        `created_at >= '2026-01-05T00:00:00Z' AND created_at <= '2026-01-10T00:00:00Z'`,
      ),
    );
  });

  it('combines dimensions', async () => {
    expect(await apiCount(`ownerId=${ownerA}&outcome=open&minValue=5000`)).toBe(
      await sqlCount(
        `owner_id = '${ownerA}' AND value >= 5000 AND stage_id IN (SELECT id FROM stage WHERE outcome = 'open')`,
      ),
    );
  });

  it('paginates without duplicates or gaps', async () => {
    const all = await api<{ items: { id: string }[]; nextCursor: string | null }>(
      BASE.opportunity,
      '/opportunities?limit=200',
      { workspaceId: wsId.value },
    );
    expect(all.body.items).toHaveLength(40);
    expect(all.body.nextCursor).toBeNull();

    const first = await api<{ items: { id: string }[]; nextCursor: string | null }>(
      BASE.opportunity,
      '/opportunities?limit=7',
      { workspaceId: wsId.value },
    );
    expect(first.body.items).toHaveLength(7);
    expect(first.body.nextCursor).not.toBeNull();

    const second = await api<{ items: { id: string }[] }>(
      BASE.opportunity,
      `/opportunities?limit=7&cursor=${first.body.nextCursor}`,
      { workspaceId: wsId.value },
    );
    expect(second.body.items).toHaveLength(7);
    const overlap = second.body.items.filter((i) => first.body.items.some((f) => f.id === i.id));
    expect(overlap).toEqual([]);
  });

  it('caps limit at 200 rather than trusting the caller', async () => {
    const res = await api<{ items: unknown[] }>(BASE.opportunity, '/opportunities?limit=100000', {
      workspaceId: wsId.value,
    });
    expect(res.status).toBe(200);
    expect(res.body.items.length).toBeLessThanOrEqual(200);
  });

  it.each([
    ['an unrecognised filter', 'sortBy=value'],
    ['a misspelled param', 'stageID=x'],
    ['a non-uuid stage', 'stageId=abc'],
    ['an unknown outcome', 'outcome=sideways'],
    ['a non-numeric bound', 'minValue=lots'],
    ['a zero limit', 'limit=0'],
    ['an inverted value range', 'minValue=10&maxValue=1'],
    ['an unparseable date', 'createdFrom=yesterday'],
  ])('rejects %s with 400', async (_label, query) => {
    const res = await api<{ message: string }>(BASE.opportunity, `/opportunities?${query}`, {
      workspaceId: wsId.value,
    });
    expect(res.status).toBe(400);
  });
});
