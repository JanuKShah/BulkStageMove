import { NestFactory } from '@nestjs/core';
import { WorkerModule } from './worker.module';

/**
 * The batch worker. No HTTP surface: it consumes batches from RabbitMQ and
 * writes them to the database. It is a separate process from transition-service
 * so that worker throughput and API availability are scaled and restarted
 * independently, and a wedged consumer cannot take the API down with it.
 *
 * init() runs the providers' onModuleInit, which is where the consumer starts.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(WorkerModule);
  app.enableShutdownHooks();
  // Nothing to await: the consumer holds the event loop open.
}

void bootstrap();
