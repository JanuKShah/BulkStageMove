import { currentRequestId, newRequestId } from './request-context';
import { REQUEST_ID_HEADER } from './request-id.middleware';

/**
 * Client for service-to-service calls. Uses node's built-in fetch rather than
 * @nestjs/microservice, so no transport dependency is added.
 */
export class ServiceClient {
  constructor(private readonly baseUrls: Record<string, string | undefined>) {}

  async get<T>(service: string, path: string, workspaceId: string): Promise<T> {
    return this.request<T>(service, path, workspaceId, { method: 'GET' });
  }

  async post<T>(service: string, path: string, workspaceId: string, body: unknown): Promise<T> {
    return this.request<T>(service, path, workspaceId, {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  private baseUrl(service: string): string {
    const base = this.baseUrls[service];
    if (!base) throw new Error(`no base url configured for service "${service}"`);
    return base;
  }

  private async request<T>(
    service: string,
    path: string,
    workspaceId: string,
    init: RequestInit,
  ): Promise<T> {
    // Forwarded so the whole chain carries one id. A call made outside a request -
    // a warmup, a scheduled job - mints its own rather than sending nothing,
    // because an empty header is indistinguishable from a broken propagation.
    const requestId = currentRequestId() ?? newRequestId();
    const response = await fetch(`${this.baseUrl(service)}${path}`, {
      ...init,
      headers: {
        'content-type': 'application/json',
        'x-workspace-id': workspaceId,
        [REQUEST_ID_HEADER]: requestId,
      },
      signal: AbortSignal.timeout(Number(process.env.SERVICE_CALL_TIMEOUT_MS ?? 5_000)),
    });

    if (!response.ok) {
      throw new ServiceCallError(service, response.status, await response.text(), requestId);
    }
    return (await response.json()) as T;
  }
}

export class ServiceCallError extends Error {
  constructor(
    readonly service: string,
    readonly status: number,
    readonly body: string,
    readonly requestId: string,
  ) {
    // The id is in the message as well as on the field. A thrown error usually
    // reaches a log through its string, and an id that is only a property is lost
    // the moment the error is serialised.
    super(`${service} responded ${status} (request ${requestId})`);
  }
}
