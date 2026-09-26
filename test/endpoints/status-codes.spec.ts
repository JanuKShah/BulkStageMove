import { randomUUID } from 'node:crypto';
import {
  BASE,
  api,
  createOpportunity,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  seedOpportunities,
  submitBulkMove,
  type TestWorkspace,
} from '../helpers';

/**
 * Pins the status code of every POST in the API.
 *
 * NestJS answers 201 to every POST unless told otherwise, so the default is
 * correct for a creation and wrong for everything else. That distinction used
 * to live only in each controller, and the tests asserted the framework
 * default rather than the intended code, so a move answered 201 and a
 * read-only predicate answered 201 and both looked correct.
 *
 * The rule, stated once: 201 only where the request brought a new resource
 * into existence. A command that mutates an existing resource is 200, and so
 * is a read. Adding a POST means adding a row here.
 */
describe('POST status codes', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('status');
    await seedOpportunities(ws.workspaceId, ws.stages['newLead'], 3);
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  it('POST /workspaces is 201 - it creates a workspace', async () => {
    const res = await api<{ id: string }>(BASE.workspace, '/workspaces', {
      method: 'POST',
      body: JSON.stringify({ name: 'status-contract' }),
    });
    expect(res.status).toBe(201);
    await destroyWorkspace(res.body.id);
  });

  it('POST /users is 201 - it creates a user', async () => {
    const res = await api<unknown>(BASE.user, '/users', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ name: 'contract', email: 'contract@example.com' }),
    });
    expect(res.status).toBe(201);
  });

  it('POST /opportunities is 201 - it creates an opportunity', async () => {
    const res = await api<unknown>(BASE.opportunity, '/opportunities', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ stageId: ws.stages['newLead'], name: 'contract' }),
    });
    expect(res.status).toBe(201);
  });

  it('POST /opportunities/:id/move is 200 - a move creates nothing', async () => {
    const opp = await createOpportunity(ws.workspaceId, ws.stages['newLead'], 'moves');
    const res = await api<unknown>(BASE.opportunity, `/opportunities/${opp.id}/move`, {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ toStageId: ws.stages['contacted'] }),
    });
    expect(res.status).toBe(200);
  });

  it('POST /stages/can-move is 200 - a predicate reads, it does not create', async () => {
    const res = await api<unknown>(BASE.stage, '/stages/can-move', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['newLead'], to: ws.stages['contacted'] }),
    });
    expect(res.status).toBe(200);
  });

  it('POST /stages/allowed-targets is 200 - a predicate reads, it does not create', async () => {
    const res = await api<unknown>(BASE.stage, '/stages/allowed-targets', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ from: ws.stages['newLead'] }),
    });
    expect(res.status).toBe(200);
  });

  it('POST /bulk-moves is 201 whether it created the job or replayed one', async () => {
    // The key must be supplied explicitly, otherwise each call generates a fresh
    // one and the second submission is a new job rather than a replay.
    const key = randomUUID();
    const filter = { targetStageId: ws.stages['contacted'] };

    const first = await submitBulkMove(ws.workspaceId, filter, key);
    expect(first.status).toBe(201);
    expect(first.body.replay).toBe(false);

    // Same status on the retry. A replay is not a lesser success - the job
    // exists and the client is holding it - and a status that changed between
    // the original and the retry would make the contract only half idempotent.
    // The body is what distinguishes them.
    const replay = await submitBulkMove(ws.workspaceId, filter, key);
    expect(replay.status).toBe(201);
    expect(replay.body.replay).toBe(true);
    expect(replay.body.jobId).toBe(first.body.jobId);
  });
});
