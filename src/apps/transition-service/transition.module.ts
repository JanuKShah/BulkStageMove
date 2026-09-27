import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RequestContextModule } from '../../shared/http/request-context.module';
import { ServiceClient } from '../../shared/http/service-client';
import { HealthController } from '../../shared/health/health.controller';
import { BulkJobRepository } from './bulk-job.repository';
import { JobTransitionRepository } from './job-transition.repository';
import { OutboxRelay } from './outbox-relay';
import { OutboxRepository } from './outbox.repository';
import { RabbitBatchPublisher } from './rabbit-batch.publisher';
import { RabbitService } from '../../shared/rabbit/rabbit.service';
import { RABBIT_CONFIG, rabbitConfig } from '../../shared/rabbit/rabbit.config';
import { TransitionController } from './transition.controller';
import { TransitionService } from './transition.service';

@Module({
  imports: [RequestContextModule],
  controllers: [TransitionController, HealthController],
  providers: [
    TransitionService,
    BulkJobRepository,
    JobTransitionRepository,
    OutboxRepository,
    OutboxRelay,
    RabbitBatchPublisher,
    RabbitService,
    DatabaseService,
    { provide: RABBIT_CONFIG, useFactory: () => rabbitConfig() },
    {
      provide: ServiceClient,
      useFactory: () =>
        new ServiceClient({
          stage: process.env.STAGE_SERVICE_URL,
          user: process.env.USER_SERVICE_URL,
          workspace: process.env.WORKSPACE_SERVICE_URL,
        }),
    },
  ],
})
export class TransitionModule {}
