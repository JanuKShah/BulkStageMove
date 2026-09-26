import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { DatabaseService } from '../database/database.service';

/**
 * Readiness probe. Answers one question: can this instance serve traffic?
 *
 * It checks the database, because a process that is running but cannot reach
 * Postgres cannot do anything useful, and `docker compose ps` should say so.
 *
 * There is deliberately no liveness probe here. A liveness check that also
 * failed on a database outage would make an orchestrator restart every healthy
 * service, turning a recoverable dependency blip into a total outage - and there
 * is no orchestrator in this project to act on one regardless. If one is added
 * later, /health/live should be a dependency-free check and this endpoint
 * should stay the readiness signal.
 *
 * Shared by all four services: identical behaviour is easier to reason about
 * than four slightly different implementations.
 */
@Controller('health')
export class HealthController {
  constructor(private readonly db: DatabaseService) {}

  @Get('ready')
  async ready(): Promise<{ status: string; database: string }> {
    try {
      await this.db.ping();
      return { status: 'ok', database: 'up' };
    } catch {
      throw new ServiceUnavailableException({ status: 'degraded', database: 'down' });
    }
  }
}
