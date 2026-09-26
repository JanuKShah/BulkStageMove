import {
  BASE,
  api,
  createOpportunity,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  type TestWorkspace,
} from '../helpers';

interface Opportunity {
  id: string;
  stage_id: string;
  name: string;
  value: string;
  owner_id: string | null;
}
interface Transition {
  from_stage_id: string | null;
  to_stage_id: string;
}

describe('opportunity-service', () => {
  let ws: TestWorkspace;
  let ownerId: string;

  beforeAll(async () => {
    ws = await provisionWorkspace('opps');
    const user = await api<{ id: string }>(BASE.user, '/users', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ name: 'Owner One' }),
    });
    ownerId = user.body.id;
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  describe('POST /opportunities', () => {
    it('creates an opportunity and defaults value to 0', async () => {
      const res = await api<Opportunity>(BASE.opportunity, '/opportunities', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ stageId: ws.stages['newLead'], name: 'no value given' }),
      });
      expect(res.status).toBe(201);
      expect(res.body.stage_id).toBe(ws.stages['newLead']);
      expect(Number(res.body.value)).toBe(0);
    });

    it('accepts a negative value, since credits and refunds are legitimate', async () => {
      const res = await api<Opportunity>(BASE.opportunity, '/opportunities', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ stageId: ws.stages['newLead'], name: 'credit', value: -2500 }),
      });
      expect(res.status).toBe(201);
      expect(Number(res.body.value)).toBe(-2500);
    });

    it('attaches an owner', async () => {
      const res = await api<Opportunity>(BASE.opportunity, '/opportunities', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ stageId: ws.stages['newLead'], name: 'owned', ownerId }),
      });
      expect(res.body.owner_id).toBe(ownerId);
    });

    it.each([
      ['missing name', { stageId: 'x' }],
      ['missing stageId', { name: 'x' }],
      ['empty body', {}],
    ])('rejects %s with 400', async (_label, body) => {
      const res = await api<{ message: string }>(BASE.opportunity, '/opportunities', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(400);
    });

    it('rejects a stage from another workspace at the database level', async () => {
      const other = await provisionWorkspace('opps-other');
      const res = await api<{ message: string }>(BASE.opportunity, '/opportunities', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ stageId: other.stages['newLead'], name: 'cross tenant' }),
      });
      expect(res.status).toBeGreaterThanOrEqual(400);
      await destroyWorkspace(other.workspaceId);
    });
  });

  describe('POST /opportunities/:id/move', () => {
    it('moves to a permitted stage', async () => {
      const opp = await createOpportunity(ws.workspaceId, ws.stages['newLead'], 'movable');
      const res = await api<Opportunity>(BASE.opportunity, `/opportunities/${opp.id}/move`, {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
      });
      expect(res.status).toBe(201);
      expect(res.body.stage_id).toBe(ws.stages['contacted']);
    });

    it('rejects a forbidden move with 409', async () => {
      const opp = await createOpportunity(ws.workspaceId, ws.stages['contacted'], 'stuck');
      const res = await api<{ message: string }>(
        BASE.opportunity,
        `/opportunities/${opp.id}/move`,
        {
          method: 'POST',
          workspaceId: ws.workspaceId,
          body: JSON.stringify({ toStageId: ws.stages['newLead'] }),
        },
      );
      expect(res.status).toBe(409);
    });

    it('rejects a missing toStageId with 400', async () => {
      const opp = await createOpportunity(ws.workspaceId, ws.stages['newLead'], 'no target');
      const res = await api<{ message: string }>(
        BASE.opportunity,
        `/opportunities/${opp.id}/move`,
        { method: 'POST', workspaceId: ws.workspaceId, body: JSON.stringify({}) },
      );
      expect(res.status).toBe(400);
    });

    it('rejects a malformed opportunity id with 400', async () => {
      const res = await api<{ message: string }>(BASE.opportunity, '/opportunities/nope/move', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
      });
      expect(res.status).toBe(400);
    });

    it('returns 404 for an unknown opportunity', async () => {
      const res = await api<{ message: string }>(
        BASE.opportunity,
        '/opportunities/11111111-1111-1111-1111-111111111111/move',
        {
          method: 'POST',
          workspaceId: ws.workspaceId,
          body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
        },
      );
      expect(res.status).toBe(404);
    });
  });

  describe('GET /opportunities/:id/transitions', () => {
    it('returns the creation transition first, then each move', async () => {
      const opp = await createOpportunity(ws.workspaceId, ws.stages['newLead'], 'audited');
      await api(BASE.opportunity, `/opportunities/${opp.id}/move`, {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
      });
      await api(BASE.opportunity, `/opportunities/${opp.id}/move`, {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ toStageId: ws.stages['closedWon'] }),
      });

      const res = await api<Transition[]>(
        BASE.opportunity,
        `/opportunities/${opp.id}/transitions`,
        { workspaceId: ws.workspaceId },
      );
      expect(res.status).toBe(200);
      expect(res.body).toHaveLength(3);
      expect(res.body[0]!.from_stage_id).toBeNull();
      expect(res.body[0]!.to_stage_id).toBe(ws.stages['newLead']);
      expect(res.body[1]!.to_stage_id).toBe(ws.stages['contacted']);
      expect(res.body[2]!.to_stage_id).toBe(ws.stages['closedWon']);
    });
  });
});
