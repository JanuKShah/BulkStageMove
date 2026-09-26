import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { HealthController } from '../../shared/health/health.controller';
import { ServiceClient } from '../../shared/http/service-client';
import { OpportunityController } from './opportunity.controller';
import { OpportunityRepository } from './opportunity.repository';
import { OpportunityService } from './opportunity.service';

@Module({
  controllers: [HealthController, OpportunityController],
  providers: [
    OpportunityService,
    OpportunityRepository,
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
export class OpportunityModule {}
