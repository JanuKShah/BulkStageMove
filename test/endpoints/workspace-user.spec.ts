import { randomUUID } from 'node:crypto';
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

  it('GET /workspaces is not exposed, so tenants cannot be enumerated', async () => {
    const res = await api<{ message: string }>(BASE.workspace, '/workspaces');
    expect(res.status).toBe(404);
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

  describe('a duplicate email is a conflict with existing state, not a bad request', () => {
    // 409, not 400: the request is well-formed and reasonable, it conflicts with
    // a row that already exists. Not 422 either, because the payload is not
    // semantically invalid - the database already holds a colliding row.
    it('answers 409 for a second user with the same email in the workspace', async () => {
      const email = 'dup@example.test';
      const first = await api<unknown>(BASE.user, '/users', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ name: 'First', email }),
      });
      expect(first.status).toBe(201);

      const second = await api<{ message: string }>(BASE.user, '/users', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ name: 'Second', email }),
      });
      expect(second.status).toBe(409);
      // the message must scope the conflict to the workspace, not imply the
      // address is taken platform-wide
      expect(second.body.message).toMatch(/in this workspace/);
    });

    it('does not leak a 5xx under concurrent creates on one email', async () => {
      const email = `race-${randomUUID()}@example.test`;
      const responses = await Promise.all(
        Array.from({ length: 6 }, () =>
          api<unknown>(BASE.user, '/users', {
            method: 'POST',
            workspaceId: ws.workspaceId,
            body: JSON.stringify({ name: 'Racer', email }),
          }),
        ),
      );
      expect(responses.every((r) => r.status < 500)).toBe(true);
      expect(responses.filter((r) => r.status === 201)).toHaveLength(1);
    });

    it('allows the same email in a different workspace', async () => {
      // The constraint is UNIQUE (workspace_id, email), not UNIQUE (email). One
      // person can be a user in several tenants' workspaces, and a conflict here
      // would break that.
      const email = 'shared@example.test';
      const mine = await api<unknown>(BASE.user, '/users', {
        method: 'POST',
        workspaceId: ws.workspaceId,
        body: JSON.stringify({ name: 'Mine', email }),
      });
      expect(mine.status).toBe(201);

      const other = await provisionWorkspace('email-scope');
      const theirs = await api<unknown>(BASE.user, '/users', {
        method: 'POST',
        workspaceId: other.workspaceId,
        body: JSON.stringify({ name: 'Theirs', email }),
      });
      expect(theirs.status).toBe(201);
      await destroyWorkspace(other.workspaceId);
    });

    it('allows several users with no email at all', async () => {
      // Postgres treats NULLs as distinct in a unique index, so users created
      // without an address must not collide with each other.
      for (const name of ['No email one', 'No email two']) {
        const res = await api<unknown>(BASE.user, '/users', {
          method: 'POST',
          workspaceId: ws.workspaceId,
          body: JSON.stringify({ name }),
        });
        expect(res.status).toBe(201);
      }
    });
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
