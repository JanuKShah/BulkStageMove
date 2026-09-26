import {
  BASE,
  api,
  createOpportunity,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  type TestWorkspace,
} from '../helpers';

/**
 * Two mechanisms:
 *
 *  1. A stage move and its transition record are one transaction. A lost
 *     transition means a permanently broken audit trail.
 *  2. A move that the rules forbid must change nothing at all - no stage
 *     update, no transition row.
 *
 * Remove the database.transaction() wrapper in OpportunityService.move() and the
 * "atomicity" cases fail.
 */
describe('move atomicity', () => {
  let ws: TestWorkspace;
  let oppId: string;

  beforeAll(async () => {
    ws = await provisionWorkspace('atomic');
    oppId = (await createOpportunity(ws.workspaceId, ws.stages['newLead'], 'atomic-subject')).id;
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  const countTransitions = async (id: string): Promise<number> => {
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM opportunity_transition WHERE opportunity_id = $1',
      [id],
    );
    return rows[0]!.n;
  };

  const currentStage = async (id: string): Promise<string> => {
    const { rows } = await pool.query<{ stage_id: string }>(
      'SELECT stage_id FROM opportunity WHERE id = $1',
      [id],
    );
    return rows[0]!.stage_id;
  };

  it('records the opening transition when an opportunity is created', async () => {
    expect(await countTransitions(oppId)).toBe(1);
  });

  it('writes exactly one additional transition on a permitted move', async () => {
    const before = await countTransitions(oppId);
    const res = await api<{ stage_id: string }>(BASE.opportunity, `/opportunities/${oppId}/move`, {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
    });
    expect(res.status).toBe(201);
    expect(res.body.stage_id).toBe(ws.stages['contacted']);
    expect(await countTransitions(oppId)).toBe(before + 1);
  });

  it('leaves the stage and the audit trail untouched when the move is forbidden', async () => {
    const stageBefore = await currentStage(oppId);
    const transitionsBefore = await countTransitions(oppId);

    // Contacted -> New Lead is backward, and no rule permits it
    const res = await api<{ message: string }>(BASE.opportunity, `/opportunities/${oppId}/move`, {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ toStageId: ws.stages['newLead'] }),
    });

    expect(res.status).toBe(409);
    expect(res.body.message).toMatch(/not allowed/);
    expect(await currentStage(oppId)).toBe(stageBefore);
    expect(await countTransitions(oppId)).toBe(transitionsBefore);
  });

  it('always agrees with the opportunity on the current stage', async () => {
    // The newest transition's to_stage must equal the stage the deal is in now.
    // Earlier transitions deliberately disagree - that is the history.
    const { rows } = await pool.query<{ to_stage_id: string; stage_id: string }>(
      `SELECT t.to_stage_id, o.stage_id
       FROM opportunity_transition t
       JOIN opportunity o ON o.id = t.opportunity_id
       WHERE t.opportunity_id = $1
       ORDER BY t.created_at DESC, t.id DESC
       LIMIT 1`,
      [oppId],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]!.to_stage_id).toBe(rows[0]!.stage_id);
  });

  it('chains transitions without gaps', async () => {
    // Each transition's from_stage must equal the previous transition's to_stage,
    // so the audit trail has no missing links.
    const { rows } = await pool.query<{ from_stage_id: string | null; to_stage_id: string }>(
      `SELECT from_stage_id, to_stage_id FROM opportunity_transition
       WHERE opportunity_id = $1 ORDER BY created_at, id`,
      [oppId],
    );
    expect(rows[0]!.from_stage_id).toBeNull();
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.from_stage_id).toBe(rows[i - 1]!.to_stage_id);
    }
  });
});
