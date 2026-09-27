import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RequestContextModule } from '../../shared/http/request-context.module';
import { HealthController } from '../../shared/health/health.controller';
import { WorkspaceController } from './workspace.controller';
import { WorkspaceRepository } from './workspace.repository';
import { WorkspaceService } from './workspace.service';

@Module({
  imports: [RequestContextModule],
  controllers: [HealthController, WorkspaceController],
  providers: [WorkspaceService, WorkspaceRepository, DatabaseService],
})
export class WorkspaceModule {}
