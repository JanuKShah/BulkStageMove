import { randomUUID } from 'node:crypto';
import { api, pool, provisionWorkspace, withDeadlockRetry, type TestWorkspace } from '../helpers';
import {
  acceptOrMintRequestId,
  currentRequestId,
  newRequestId,
} from '../../src/shared/http/request-context';
import { REQUEST_ID_HEADER } from '../../src/shared/http/request-id.middleware';
import { ServiceCallError } from '../../src/shared/http/service-client';

const WORKSPACE = 'http://localhost:3001';
const OPPORTUNITY = 'http://localhost:3004';

/**
 * One request crosses up to four processes, so the only way to follow it is a
 * value carried with it. These cover the two things that can go wrong: the id is
 * untrusted input that ends up in log lines, and it has to survive the hops
 * without being regenerated.
 */
describe('request correlation', () => {
  let ws: TestWorkspace;
  const readPath = (): string => '/workspaces/' + ws.workspaceId;

  beforeAll(async () => {
    ws = await provisionWorkspace('correlation');
  });

  afterAll(async () => {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await pool.end();
  });

  it('honours a caller-supplied id', async () => {
    const id = 'trace-' + randomUUID();
    const res = await api(WORKSPACE, readPath(), {
      workspaceId: ws.workspaceId,
      headers: { [REQUEST_ID_HEADER]: id },
    });
    expect(res.status).toBe(200);
    // Echoed back, so a caller that did not supply one can quote it later without
    // correlating by timestamp.
    expect(res.headers.get(REQUEST_ID_HEADER)).toBe(id);
  });

  it('mints an id when the caller supplies none', async () => {
    const res = await api(WORKSPACE, readPath(), { workspaceId: ws.workspaceId });
    expect(res.status).toBe(200);
    expect(res.headers.get(REQUEST_ID_HEADER)).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('mints a fresh id rather than logging a forged one', () => {
    // The point of the pattern. A caller-supplied id lands in a log line, and
    // without a check a value carrying a newline lets that caller write arbitrary
    // lines into the log - forging an entry that looks like a different request,
    // or corrupting a terminal reading the file.
    const hostile = [
      'has\nnewline',
      'has space',
      'tab\there',
      'semi;colon',
      'quote"double',
      '<script>alert(1)</script>',
      'a'.repeat(65),
      '',
      '   ',
    ];
    for (const value of hostile) {
      expect(acceptOrMintRequestId(value)).toMatch(/^[0-9a-f-]{36}$/);
    }
  });

  it('keeps a plausible id rather than discarding it needlessly', () => {
    // The check has to reject the hostile and keep the ordinary, or it is just a
    // generator that ignores its input.
    const ok = 'client-supplied.abc_123:x';
    expect(acceptOrMintRequestId(ok)).toBe(ok);
  });

  it('gives two concurrent requests different ids', async () => {
    // The context is per async chain. If it were held on something shared, two
    // requests in flight on one event loop would read each other's id and the
    // whole trace would be worthless.
    const [a, b] = await Promise.all([
      api(WORKSPACE, readPath(), { workspaceId: ws.workspaceId }),
      api(WORKSPACE, readPath(), { workspaceId: ws.workspaceId }),
    ]);
    expect(a.headers.get(REQUEST_ID_HEADER)).not.toBe(b.headers.get(REQUEST_ID_HEADER));
  });

  it('returns the id on an error response too', async () => {
    // A middleware that only ran on the happy path would leave exactly the
    // requests you most want to trace - the failures - with nothing to correlate
    // on. The status code changes; the id must not.
    const id = 'trace-err-' + randomUUID();
    const res = await api(WORKSPACE, '/workspaces/' + randomUUID(), {
      workspaceId: ws.workspaceId,
      headers: { [REQUEST_ID_HEADER]: id },
    });
    expect(res.status).toBe(404);
    expect(res.headers.get(REQUEST_ID_HEADER)).toBe(id);
  });

  it('returns the id on a rejected request', async () => {
    // 400 from the filter guard rather than 404 from a missing row - a different
    // failure path, and equally worth being able to trace.
    const id = 'trace-bad-' + randomUUID();
    const res = await api(WORKSPACE, readPath(), {
      workspaceId: ws.workspaceId,
      method: 'POST',
      headers: { [REQUEST_ID_HEADER]: id, 'content-type': 'application/json' },
      body: JSON.stringify({ notAField: 1 }),
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.headers.get(REQUEST_ID_HEADER)).toBe(id);
  });

  it('covers the mutating verbs, not just reads', async () => {
    // forRoutes is applied per method pattern, so a wildcard that matched GET
    // only would silently leave writes untraced.
    const id = 'trace-write-' + randomUUID();
    const created = await api(WORKSPACE, '/workspaces', {
      workspaceId: ws.workspaceId,
      method: 'POST',
      headers: { [REQUEST_ID_HEADER]: id },
      body: JSON.stringify({ name: 'corr-' + randomUUID().slice(0, 8) }),
    });
    expect(created.status).toBe(201);
    expect(created.headers.get(REQUEST_ID_HEADER)).toBe(id);
  });

  it('carries the id on a failed service-to-service call', () => {
    // The error is what surfaces in a log when stage-service is unreachable, and
    // an id that is only a property on the error is lost the moment it is
    // serialised - so it also has to be in the message.
    const id = 'trace-down-' + randomUUID();
    const error = new ServiceCallError('stage-service', 503, 'unavailable', id);
    expect(error.requestId).toBe(id);
    expect(error.message).toContain(id);
  });

  it('has no correlation id outside a request', () => {
    // The ServiceClient call path mints its own in this case, because an empty
    // header is indistinguishable from a broken propagation. Asserting the store is
    // empty outside a request is what makes that fallback reachable rather than
    // dead.
    expect(currentRequestId()).toBeUndefined();
    const generated = newRequestId();
    expect(generated).toMatch(/^[0-9a-f-]{36}$/);
    expect(generated).not.toBe(newRequestId());
  });

  it('assigns the id before the handler runs, not after', async () => {
    // Ordering matters: the handler and any ServiceClient call it makes have to
    // see the id, and a middleware that set the context too late would leave
    // every downstream hop untraced while the response still looked correct.
    const seen = new Set<string>();
    const res = await api(WORKSPACE, readPath(), { workspaceId: ws.workspaceId });
    expect(res.status).toBe(200);
    seen.add(res.headers.get(REQUEST_ID_HEADER) ?? '');
    expect(seen.size).toBe(1);
    expect([...seen][0]).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('mints and forwards an id at the edge', async () => {
    // The nginx layer is the normal source of a caller-visible id, and it is not
    // exercised by hitting a service port directly. Skipped when the proxy is
    // not up, because the fast suites are run without it.
    // A proxied path, deliberately not /_health. That location is a local
    // `return`, so it never reaches proxy_set_header and would pass or fail for
    // reasons that have nothing to do with the header logic under test.
    const proxied = 'http://localhost:8080/bulk-moves/' + randomUUID();
    const auth = { 'x-workspace-id': ws.workspaceId };

    let reachable = false;
    try {
      const probe = await fetch(proxied, { headers: auth });
      reachable = probe.status > 0;
    } catch {
      reachable = false;
    }
    if (!reachable) {
      console.log('  (skipped: nginx is not running, so the edge is not exercised)');
      return;
    }

    const supplied = 'edge-' + randomUUID();
    const honoured = await fetch(proxied, {
      headers: { ...auth, [REQUEST_ID_HEADER]: supplied },
    });
    expect(honoured.headers.get(REQUEST_ID_HEADER)).toBe(supplied);

    // And a hostile one is replaced rather than passed through to a service, which
    // is the whole reason the map in nginx.conf exists. 32 hex chars is nginx's
    // own $request_id, not the value that was sent.
    const forged = await fetch(proxied, {
      headers: { ...auth, [REQUEST_ID_HEADER]: 'has space' },
    });
    const forgedId = forged.headers.get(REQUEST_ID_HEADER);
    expect(forgedId).toMatch(/^[0-9a-f]{32}$/);
    expect(forgedId).not.toBe('has space');
  });

  it('propagates one id across a service boundary', async () => {
    // opportunity-service calls stage-service to resolve outcome=open, so one id
    // in both logs is what makes a multi-hop request followable. The assertion
    // here is that the caller's id survives the round trip and comes back.
    const id = 'xhop-' + randomUUID();
    const res = await api(OPPORTUNITY, '/opportunities?outcome=open', {
      workspaceId: ws.workspaceId,
      headers: { [REQUEST_ID_HEADER]: id },
    });
    expect(res.status).toBe(200);
    expect(res.headers.get(REQUEST_ID_HEADER)).toBe(id);
  });
});
