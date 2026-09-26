import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { HealthController } from '../../shared/health/health.controller';
import { UserController } from './user.controller';
import { UserRepository } from './user.repository';
import { UserService } from './user.service';

@Module({
  controllers: [HealthController, UserController],
  providers: [UserService, UserRepository, DatabaseService],
})
export class UserModule {}
