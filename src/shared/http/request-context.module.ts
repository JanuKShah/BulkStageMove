import { MiddlewareConsumer, Module, NestModule, RequestMethod } from '@nestjs/common';
import { RequestIdMiddleware } from './request-id.middleware';

/**
 * Applies the correlation id middleware to every route in the service.
 *
 * A shared module rather than a `consumer.apply` line in each of the five HTTP
 * services, because the one thing that must not drift is the header name and the
 * minting rule - five copies of that is five chances to disagree.
 *
 * The worker has no HTTP surface, so it does not import this: a message it
 * consumes carries a job and batch, not a request, and correlation does not apply.
 */
@Module({})
export class RequestContextModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer
      .apply(RequestIdMiddleware)
      .forRoutes({ path: '*path', method: RequestMethod.ALL });
  }
}
