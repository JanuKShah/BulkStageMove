import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import type { ConsumeMessage } from 'amqplib';
import { RabbitService } from '../../shared/rabbit/rabbit.service';
import {
  RABBIT_CONFIG,
  type BatchMessage,
  type RabbitConfig,
} from '../../shared/rabbit/rabbit.config';
import { type BatchResult, WorkerRepository } from './worker.repository';

/**
 * Consumes batch messages and applies them.
 *
 * All the decision-making is in WorkerRepository.processBatch. This class only
 * turns its answer into broker behaviour, and does so by republishing rather
 * than nacking with requeue: requeue puts a message back immediately with no
 * delay and no attempt count, which spins.
 */
@Injectable()
export class BatchWorker implements OnModuleInit, OnModuleDestroy {
  private channel: Awaited<ReturnType<RabbitService['consumerChannel']>> | null = null;
  private stopping = false;

  constructor(
    private readonly rabbit: RabbitService,
    private readonly repository: WorkerRepository,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  async onModuleInit(): Promise<void> {
    this.channel = await this.rabbit.consumerChannel();
    await this.rabbit.declareTopology(this.channel);
    // One batch in flight per consumer: a batch is 1000 records, so a higher
    // prefetch does not add throughput, it just holds whole batches in memory.
    await this.channel.prefetch(this.config.prefetch);
    await this.channel.consume(this.config.queue, (message) => {
      if (message) void this.handle(message);
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    try {
      await this.channel?.close();
    } catch {
      // shutting down
    }
  }

  /** Exposed so tests can drive one message without a live consumer. */
  async handle(message: ConsumeMessage): Promise<BatchResult> {
    let batch: BatchMessage;
    try {
      batch = JSON.parse(message.content.toString()) as BatchMessage;
    } catch {
      // Unparseable will never succeed on a retry, so it goes straight to the DLQ.
      const result: BatchResult = { disposition: 'dead', moved: 0, attempts: 0 };
      await this.deadLetter(message);
      return result;
    }

    let result: BatchResult;
    try {
      result = await this.repository.processBatch(batch.workspaceId, batch.jobId, batch.batchNo);
    } catch {
      // Infrastructure failure, nothing was touched. Retried without spending
      // the attempt budget: a database outage should not be what dead-letters a
      // batch. The reason this is safe is that the lock is released and the
      // items are still pending, so a redelivery starts cleanly.
      result = { disposition: 'retry', moved: 0, attempts: 0 };
    }

    if (result.disposition === 'retry' || result.disposition === 'dead') {
      await this.republish(message, result);
    }
    this.channel?.ack(message);
    return result;
  }

  private async republish(message: ConsumeMessage, result: BatchResult): Promise<void> {
    const dead = result.disposition === 'dead' || result.attempts >= this.config.maxAttempts;
    const queue = dead ? this.config.deadLetterQueue : this.rabbit.retryQueueFor(result.attempts);
    const channel = await this.rabbit.publisherChannel();
    const properties = message.properties as { contentType?: string; messageId?: string };
    channel.publish('', queue, message.content, {
      persistent: true,
      contentType: properties.contentType ?? 'application/json',
      ...(properties.messageId === undefined ? {} : { messageId: properties.messageId }),
      headers: {
        'x-attempt': result.attempts + 1,
        ...(result.reason ? { reason: result.reason } : {}),
      },
    });
    await channel.waitForConfirms();
  }

  private async deadLetter(message: ConsumeMessage): Promise<void> {
    const channel = await this.rabbit.publisherChannel();
    channel.publish('', this.config.deadLetterQueue, message.content, {
      persistent: true,
      contentType: 'application/json',
    });
    await channel.waitForConfirms();
  }

  get isStopping(): boolean {
    return this.stopping;
  }
}
