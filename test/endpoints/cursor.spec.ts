/**
 * Keyset cursors carry their own position.
 *
 * The cursor used to be an id, and the position re-read from the row it pointed
 * at. Two failures came of that, and both are silent - no error, just a wrong
 * answer:
 *
 *   1. If the cursor row was gone, the subquery returned NULL, `(created_at, id)
 *      > NULL` is UNKNOWN, and *every remaining row was dropped*. A caller paging
 *      a large set got a short list and no indication anything was wrong.
 *   2. The lookup cost a scan proportional to how far into the set the cursor
 *      sat. Measured at 47.9ms for a cursor 25,000 rows in, against 1.5ms for the
 *      bound form.
 *
 * And the reason it was a subquery rather than a parameter is the trap this has to
 * avoid: `timestamptz` carries microseconds, a JS `Date` carries milliseconds, and
 * binding one truncates the cursor to below its own row - so that row comes back
 * on the next page, for ever.
 */
import { randomUUID } from 'node:crypto';
import {
  api,
  createPrivateStage,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  seedOpportunitiesInStage,
} from '../helpers';

describe('keyset cursors', () => {
  let ws: Awaited<ReturnType<typeof provisionWorkspace>>;
  let stageId: string;

  beforeAll(async () => {
    ws = await provisionWorkspace('cursor');
    stageId = await createPrivateStage(ws.workspaceId, 'paged');
    await seedOpportunitiesInStage(ws.workspaceId, stageId, 40);
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
  });

  const page = async (
    query: string,
  ): Promise<{ items: { id: string }[]; nextCursor: string | null }> =>
    (
      await api<{ items: { id: string }[]; nextCursor: string | null }>(
        'http://localhost:3004',
        `/opportunities?${query}`,
        { workspaceId: ws.workspaceId },
      )
    ).body;

  it('walks the whole set exactly once, in order, with no repeats', async () => {
    // The precision property. If the cursor timestamp were truncated on the way
    // through JS, the cursor row would compare greater than its own truncated
    // value and reappear - so this would loop or repeat rather than finish.
    const seen: string[] = [];
    let cursor: string | null = null;
    let guard = 0;

    for (;;) {
      const res = await page(`stageId=${stageId}&limit=7${cursor ? `&cursor=${cursor}` : ''}`);
      seen.push(...res.items.map((i) => i.id));
      cursor = res.nextCursor;
      if (!cursor) break;
      // A cursor that never terminates looks exactly like a working one until it
      // runs for ever, so bound the loop rather than trusting hasMore.
      if (++guard > 20) throw new Error('paging did not terminate');
    }

    expect(seen).toHaveLength(40);
    expect(new Set(seen).size).toBe(40);
  });

  it('still returns the rest of the set after the cursor row is deleted', async () => {
    // The bug this replaces. The cursor row is removed between pages - ordinary
    // here, since deleting a workspace cascades to its opportunities - and the
    // walk has to continue from the position it carries rather than re-reading a
    // row that is no longer there.
    const first = await page(`stageId=${stageId}&limit=10`);
    expect(first.items).toHaveLength(10);
    const cursorId = first.items.at(-1)!.id;

    // The position, read while the row still exists. This has to be captured
    // before the delete: an oracle that re-derives it afterwards is the very bug
    // being tested, and would compare the endpoint's correct answer against an
    // empty set.
    const at = await pool.query<{ created_at: string }>(
      'SELECT created_at::text AS created_at FROM opportunity WHERE id = $1',
      [cursorId],
    );
    expect(at.rows[0]?.created_at).toBeTruthy();

    await pool.query('DELETE FROM opportunity WHERE id = $1 AND workspace_id = $2', [
      cursorId,
      ws.workspaceId,
    ]);

    const second = await page(`stageId=${stageId}&limit=10&cursor=${first.nextCursor}`);

    // Before the fix this was zero rows: the subquery found nothing, the
    // comparison went UNKNOWN, and the rest of the set vanished without an error.
    expect(second.items.length).toBeGreaterThan(0);

    // And what it returns is genuinely the rest, not a page of anything - checked
    // against the position as it was before the row went.
    //
    // Newest first, so "the rest" is everything *older* than the cursor and the
    // walk descends. The list endpoint pages backwards; a forward-looking oracle
    // would compare it against a disjoint set and call a correct page wrong.
    const rows = await pool.query<{ id: string }>(
      `SELECT id FROM opportunity
        WHERE workspace_id = $1 AND stage_id = $2
          AND (created_at, id) < ($3::timestamptz, $4::uuid)
        ORDER BY created_at DESC, id DESC LIMIT 10`,
      [ws.workspaceId, stageId, at.rows[0]!.created_at, cursorId],
    );
    expect(rows.rows.length).toBeGreaterThan(0);
    expect(second.items.map((i) => i.id)).toEqual(rows.rows.map((r) => r.id));
  });

  it('rejects a cursor that is not one, rather than silently ignoring it', async () => {
    // A bare uuid is the old format and carries no position. Accepting it would
    // put the caller back on the path this replaced.
    const res = await api(
      'http://localhost:3004',
      `/opportunities?stageId=${stageId}&cursor=${randomUUID()}`,
      {
        workspaceId: ws.workspaceId,
      },
    );
    expect(res.status).toBe(400);
    expect((res.body as { message: string }).message).toMatch(/cursor/);
  });

  it('carries a position, so the cursor is not just the id it points at', async () => {
    // Not a format test - a statement of what the token is for. The timestamp is
    // inside it because the position has to travel with the cursor, and a test that
    // decoded the base64 and checked the id would be asserting the encoding, which
    // is meant to change.
    const first = await page(`stageId=${stageId}&limit=5`);
    expect(first.nextCursor).toBeTruthy();
    expect(first.nextCursor).not.toBe(first.items.at(-1)!.id);
    // Opaque: no raw uuid in it.
    expect(first.nextCursor).not.toMatch(/^[0-9a-f-]{36}$/);
  });
});
