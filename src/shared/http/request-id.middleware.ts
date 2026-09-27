import { Injectable, Logger, NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { acceptOrMintRequestId, runWithRequestId } from './request-context';

export const REQUEST_ID_HEADER = 'x-request-id';

/**
 * Attaches a correlation id to every request and logs its lifetime.
 *
 * The id is taken from the incoming header when one is present and safe, and
 * minted otherwise, so the same id is used end to end whether the caller supplied
 * it or not. The edge proxy is the normal source; this is the backstop for
 * requests that reach a service directly, which in practice means the tests.
 *
 * One line on the way in and one on the way out is enough to follow a request
 * across services. Anything more would be log volume for its own sake - the
 * per-service detail belongs in the service that has it, keyed by this id.
 *
 * The response carries the id back, so a caller that did not send one can quote
 * it when reporting a problem without having to correlate by timestamp.
 */
@Injectable()
export class RequestIdMiddleware implements NestMiddleware {
  private readonly logger = new Logger('http');

  use(req: Request & { requestId?: string }, res: Response, next: NextFunction): void {
    const incoming = req.headers[REQUEST_ID_HEADER];
    const requestId = acceptOrMintRequestId(
      Array.isArray(incoming) ? incoming[0] : incoming,
    );
    req.requestId = requestId;
    res.setHeader(REQUEST_ID_HEADER, requestId);

    const started = process.hrtime.bigint();
    const method = req.method;
    const path = req.originalUrl.split('?')[0] ?? req.originalUrl;
    const workspace = req.headers['x-workspace-id'];

    // Everything downstream - guards, interceptors, the controller, and any
    // ServiceClient call the handler makes - runs inside this.
    runWithRequestId(requestId, () => {
      this.logger.log(
        `--> ${method} ${path}${workspace ? ` ws=${String(workspace).slice(0, 8)}` : ''} id=${requestId}`,
      );
      res.on('finish', () => {
        const ms = Number(process.hrtime.bigint() - started) / 1e6;
        this.logger.log(
          `<-- ${method} ${path} ${res.statusCode} ${ms.toFixed(1)}ms id=${requestId}`,
        );
      });
      next();
    });
  }
}
