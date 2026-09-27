import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RequestContextModule } from '../../shared/http/request-context.module';
import { HealthController } from '../../shared/health/health.controller';
import { UserController } from './user.controller';
import { UserRepository } from './user.repository';
import { UserService } from './user.service';

@Module({
  imports: [RequestContextModule],
  controllers: [HealthController, UserController],
  providers: [UserService, UserRepository, DatabaseService],
})
export class UserModule {}
