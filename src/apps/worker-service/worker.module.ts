import { Module } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RABBIT_CONFIG, rabbitConfig } from '../../shared/rabbit/rabbit.config';
import { RabbitService } from '../../shared/rabbit/rabbit.service';
import { BatchWorker } from './batch-worker.service';
import { WorkerRepository } from './worker.repository';

@Module({
  providers: [
    BatchWorker,
    WorkerRepository,
    DatabaseService,
    RabbitService,
    { provide: RABBIT_CONFIG, useFactory: () => rabbitConfig() },
  ],
})
export class WorkerModule {}
