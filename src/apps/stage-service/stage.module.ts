import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RequestContextModule } from '../../shared/http/request-context.module';
import { HealthController } from '../../shared/health/health.controller';
import { StageController } from './stage.controller';
import { StageRepository } from './stage.repository';
import { StageService } from './stage.service';

@Module({
  imports: [RequestContextModule],
  controllers: [HealthController, StageController],
  providers: [StageService, StageRepository, DatabaseService],
})
export class StageModule {}
