import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

/**
 * The correlation id for the request currently being served.
 *
 * A single request crosses up to four processes here - the edge, then
 * opportunity-service calling stage-service and user-service before it answers.
 * Without something carried alongside it, those are four unrelated log streams,
 * and the only way to connect them is a timestamp comparison, which is exactly
 * the thing that drifts.
 *
 * AsyncLocalStorage rather than a constructor parameter or a request-scoped
 * provider because the id is needed two layers below where the request is
 * handled: ServiceClient has to put it on an outgoing call, and nothing in that
 * call stack knows about HTTP. Threading it through every method signature to
 * reach it would put a transport concern into the service layer.
 *
 * Scope is per async chain, so two requests being served at once cannot read each
 * other's id even though they share an event loop and a connection pool.
 */
const storage = new AsyncLocalStorage<string>();

/**
 * A caller-supplied id is untrusted input that ends up in log lines.
 *
 * Anything outside this set is refused and a fresh id issued, because a value
 * containing a newline or an escape sequence would let a client write whatever
 * it liked into the log - forging a line that looks like a different request, or
 * corrupting a terminal reading the file. The length cap is the same reason.
 */
const SAFE = /^[A-Za-z0-9._:-]{1,64}$/;

export function acceptOrMintRequestId(candidate: string | undefined): string {
  if (candidate === undefined) return newRequestId();
  const trimmed = candidate.trim();
  return SAFE.test(trimmed) ? trimmed : newRequestId();
}

export function newRequestId(): string {
  return randomUUID();
}

export function runWithRequestId<T>(requestId: string, fn: () => T): T {
  return storage.run(requestId, fn);
}

export function currentRequestId(): string | undefined {
  return storage.getStore();
}
