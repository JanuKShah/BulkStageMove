import { pool, provisionWorkspace, destroyWorkspace, BASE, api } from '../helpers';

/**
 * Tenant isolation is enforced by the schema, not by application discipline.
 * Every reference to another tenant-owned row is a composite foreign key that
 * carries workspace_id.
 *
 * Drop opportunity_stage_fk, opportunity_owner_fk, stage_transition_rule_from_fk
 * or stage_transition_rule_to_fk from the migration and the matching test below
 * fails. That is the whole point: the guarantee lives in the database, so no
 * amount of application-layer care can route around it.
 */
describe('tenant isolation is enforced by the database', () => {
  let a: { workspaceId: string; stages: Record<string, string> };
  let b: { workspaceId: string; stages: Record<string, string> };

  beforeAll(async () => {
    a = await provisionWorkspace('iso-a');
    b = await provisionWorkspace('iso-b');
  });

  afterAll(async () => {
    await destroyWorkspace(a.workspaceId);
    await destroyWorkspace(b.workspaceId);
    await pool.end();
  });

  it('refuses an opportunity pointing at another workspace stage', async () => {
    await expect(
      pool.query(`INSERT INTO opportunity (workspace_id, stage_id, name) VALUES ($1, $2, 'leak')`, [
        b.workspaceId,
        a.stages['newLead'],
      ]),
    ).rejects.toThrow(/opportunity_stage_fk/);
  });

  it('refuses an opportunity owned by a user from another workspace', async () => {
    const { rows } = await pool.query<{ id: string }>(
      'INSERT INTO app_user (workspace_id, name, email) VALUES ($1, $2, $3) RETURNING id',
      [a.workspaceId, 'owner-in-a', 'owner-in-a@tenant.test'],
    );
    await expect(
      pool.query(
        `INSERT INTO opportunity (workspace_id, stage_id, name, owner_id) VALUES ($1, $2, 'leak', $3)`,
        [b.workspaceId, b.stages['newLead'], rows[0]!.id],
      ),
    ).rejects.toThrow(/opportunity_owner_fk/);
  });

  it('refuses a transition rule spanning two workspaces', async () => {
    await expect(
      pool.query(
        `INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id)
         VALUES ($1, $2, $3)`,
        [a.workspaceId, a.stages['newLead'], b.stages['contacted']],
      ),
    ).rejects.toThrow(/stage_transition_rule_(from|to)_fk/);
  });

  it('refuses a transition pointing at a stage in another workspace', async () => {
    await expect(
      pool.query(
        `INSERT INTO opportunity_transition (workspace_id, opportunity_id, from_stage_id, to_stage_id)
         VALUES ($1, gen_random_uuid(), $2, $3)`,
        [b.workspaceId, a.stages['newLead'], b.stages['contacted']],
      ),
    ).rejects.toThrow();
  });

  it('allows the same stage name in two different workspaces', async () => {
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM stage
       WHERE workspace_id IN ($1, $2) AND name = 'New Lead'`,
      [a.workspaceId, b.workspaceId],
    );
    // each provisioned workspace has exactly one "New Lead"
    expect(rows[0]!.n).toBe(2);
  });

  it('does not expose an endpoint that enumerates every workspace', async () => {
    // A caller must never be able to discover the other tenants on the
    // platform. This passed all 88 tests at one point while GET /workspaces
    // happily returned every workspace, so it is pinned here explicitly.
    for (const header of [a.workspaceId, b.workspaceId, '']) {
      const res = await fetch(`${BASE.workspace}/workspaces`, {
        headers: header === '' ? {} : { 'x-workspace-id': header },
      });
      expect(res.status).toBe(404);
    }
  });

  it('does not leak rows across workspaces when reading through the API', async () => {
    const created = await api<{ id: string }>(BASE.opportunity, '/opportunities', {
      method: 'POST',
      workspaceId: a.workspaceId,
      body: JSON.stringify({ stageId: a.stages['newLead'], name: 'belongs-to-a' }),
    });
    expect(created.status).toBeLessThan(400);

    // workspace B asks for A's opportunity by id
    const seenByB = await api<unknown[]>(
      BASE.opportunity,
      `/opportunities/${created.body.id}/transitions`,
      { workspaceId: b.workspaceId },
    );
    expect(seenByB.body).toEqual([]);

    // and B's own list never contains it
    const listForB = await api<{ items: { id: string }[] }>(
      BASE.opportunity,
      '/opportunities?limit=200',
      {
        workspaceId: b.workspaceId,
      },
    );
    expect(listForB.body.items.map((i) => i.id)).not.toContain(created.body.id);
  });
});
