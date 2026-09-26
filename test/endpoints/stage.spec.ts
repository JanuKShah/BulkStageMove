import {
  BASE,
  api,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  type TestWorkspace,
} from '../helpers';

describe('stage-service', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('stages');
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  it('GET /stages lists the stages with their outcome', async () => {
    const res = await api<{ id: string; name: string; outcome: string }[]>(BASE.stage, '/stages', {
      workspaceId: ws.workspaceId,
    });
    expect(res.status).toBe(200);
    expect(res.body.map((s) => s.name).sort()).toEqual(['Closed Won', 'Contacted', 'New Lead']);
    expect(res.body.find((s) => s.name === 'Closed Won')!.outcome).toBe('won');
    expect(res.body.find((s) => s.name === 'New Lead')!.outcome).toBe('open');
  });

  it('GET /stages/:id returns one stage', async () => {
    const res = await api<{ name: string }>(BASE.stage, `/stages/${ws.stages['contacted']}`, {
      workspaceId: ws.workspaceId,
    });
    expect(res.status).toBe(200);
    expect(res.body.name).toBe('Contacted');
  });

  it('GET /stages/:id returns 400 for a malformed id', async () => {
    const res = await api<{ message: string }>(BASE.stage, '/stages/nope', {
      workspaceId: ws.workspaceId,
    });
    expect(res.status).toBe(400);
  });

  it('GET /stages/:id returns 404 for an unknown uuid', async () => {
    const res = await api<{ message: string }>(
      BASE.stage,
      '/stages/11111111-1111-1111-1111-111111111111',
      { workspaceId: ws.workspaceId },
    );
    expect(res.status).toBe(404);
  });

  it('POST /stages/can-move allows a permitted forward move', async () => {
    const res = await api<{ allowed: boolean }>(BASE.stage, '/stages/can-move', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['newLead'], to: ws.stages['contacted'] }),
    });
    expect(res.status).toBe(200);
    expect(res.body.allowed).toBe(true);
  });

  it('POST /stages/can-move denies a move with no rule', async () => {
    const res = await api<{ allowed: boolean }>(BASE.stage, '/stages/can-move', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['contacted'], to: ws.stages['newLead'] }),
    });
    expect(res.status).toBe(200);
    expect(res.body.allowed).toBe(false);
  });

  it('POST /stages/can-move rejects malformed ids with 400', async () => {
    const res = await api<{ message: string }>(BASE.stage, '/stages/can-move', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: 'nope', to: 'nope' }),
    });
    expect(res.status).toBe(400);
  });

  it('POST /stages/can-move rejects a missing body with 400', async () => {
    const res = await api<{ message: string }>(BASE.stage, '/stages/can-move', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
  });

  it('POST /stages/allowed-targets returns the permitted destinations', async () => {
    const res = await api<{ to: string[] }>(BASE.stage, '/stages/allowed-targets', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['newLead'] }),
    });
    expect(res.status).toBe(200);
    expect(res.body.to).toEqual([ws.stages['contacted']]);
  });

  it('POST /stages/allowed-targets returns an empty list when nothing is permitted', async () => {
    const res = await api<{ to: string[] }>(BASE.stage, '/stages/allowed-targets', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['closedWon'] }),
    });
    expect(res.body.to).toEqual([]);
  });
});
