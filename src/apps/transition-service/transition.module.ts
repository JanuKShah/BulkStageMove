import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { ServiceClient } from '../../shared/http/service-client';
import { HealthController } from '../../shared/health/health.controller';
import { BulkJobItemRepository } from './bulk-job-item.repository';
import { BulkJobRepository } from './bulk-job.repository';
import { JobTransitionRepository } from './job-transition.repository';
import { TransitionController } from './transition.controller';
import { TransitionService } from './transition.service';

@Module({
  controllers: [TransitionController, HealthController],
  providers: [
    TransitionService,
    BulkJobRepository,
    BulkJobItemRepository,
    JobTransitionRepository,
    DatabaseService,
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
