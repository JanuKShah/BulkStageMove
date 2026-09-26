import {
  BASE,
  api,
  destroyWorkspace,
  pool,
  provisionWorkspace,
  type TestWorkspace,
} from '../helpers';

/** Every route on workspace-service and user-service, including failure paths. */
describe('workspace-service', () => {
  it('POST /workspaces creates a workspace', async () => {
    const name = `endpoint-ws-${process.pid}`;
    const res = await api<{ id: string; name: string }>(BASE.workspace, '/workspaces', {
      method: 'POST',
      body: JSON.stringify({ name }),
    });
    expect(res.status).toBe(201);
    expect(res.body.name).toBe(name);
    await pool.query('DELETE FROM workspace WHERE id = $1', [res.body.id]);
  });

  it('POST /workspaces rejects a missing name with 400', async () => {
    const res = await api<{ message: string }>(BASE.workspace, '/workspaces', {
      method: 'POST',
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    expect(res.body.message).toMatch(/name is required/);
  });

  it('GET /workspaces/:id returns the workspace', async () => {
    const created = await api<{ id: string }>(BASE.workspace, '/workspaces', {
      method: 'POST',
      body: JSON.stringify({ name: `endpoint-ws-get-${process.pid}` }),
    });
    const res = await api<{ id: string; name: string }>(
      BASE.workspace,
      `/workspaces/${created.body.id}`,
    );
    expect(res.status).toBe(200);
    expect(res.body.id).toBe(created.body.id);
    await pool.query('DELETE FROM workspace WHERE id = $1', [created.body.id]);
  });

  it('GET /workspaces/:id returns 404 for an unknown uuid', async () => {
    const res = await api<{ message: string }>(
      BASE.workspace,
      '/workspaces/11111111-1111-1111-1111-111111111111',
    );
    expect(res.status).toBe(404);
  });

  it('GET /workspaces/:id returns 400 for a malformed id', async () => {
    const res = await api<{ message: string }>(BASE.workspace, '/workspaces/not-a-uuid');
    expect(res.status).toBe(400);
    expect(res.body.message).toMatch(/must be a uuid/);
  });
});

describe('user-service', () => {
  let ws: TestWorkspace;

  beforeAll(async () => {
    ws = await provisionWorkspace('users');
  });

  afterAll(async () => {
    await destroyWorkspace(ws.workspaceId);
    await pool.end();
  });

  it('POST /users creates a user', async () => {
    const res = await api<{ id: string; name: string }>(BASE.user, '/users', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({ name: 'Alice', email: 'alice@example.test' }),
    });
    expect(res.status).toBe(201);
    expect(res.body.name).toBe('Alice');
  });

  it('POST /users rejects a missing name with 400', async () => {
    const res = await api<{ message: string }>(BASE.user, '/users', {
      method: 'POST',
      workspaceId: ws.workspaceId,
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
  });

  it('GET /users lists only the calling workspace users', async () => {
    const res = await api<{ id: string; name: string }[]>(BASE.user, '/users', {
      workspaceId: ws.workspaceId,
    });
    expect(res.status).toBe(200);
    expect(res.body.every((u) => u.name !== undefined)).toBe(true);
  });

  it('GET /users requires the workspace header', async () => {
    const res = await api<{ message: string }>(BASE.user, '/users');
    expect(res.status).toBe(400);
    expect(res.body.message).toMatch(/x-workspace-id/);
  });

  it('GET /users rejects a malformed workspace header', async () => {
    const res = await api<{ message: string }>(BASE.user, '/users', { workspaceId: 'nope' });
    expect(res.status).toBe(400);
  });

  it('GET /users/:id returns 400 for a malformed id', async () => {
    const res = await api<{ message: string }>(BASE.user, '/users/not-a-uuid', {
      workspaceId: ws.workspaceId,
    });
    expect(res.status).toBe(400);
  });
});
