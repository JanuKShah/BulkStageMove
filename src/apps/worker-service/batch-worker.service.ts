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
  private channels: Awaited<ReturnType<RabbitService['consumerChannel']>>[] = [];
  private stopping = false;

  constructor(
    private readonly rabbit: RabbitService,
    private readonly repository: WorkerRepository,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  async onModuleInit(): Promise<void> {
    // One channel per consumer slot. Multiple consumers on a single channel would
    // not help: amqplib dispatches a channel's messages one callback at a time,
    // so they would still be processed in series. Separate channels are what
    // actually gives parallel delivery.
    const slots = Math.max(1, this.config.consumerConcurrency);
    for (let i = 0; i < slots; i++) {
      const channel = await this.rabbit.consumerChannel();
      if (i === 0) await this.rabbit.declareTopology(channel);
      // One batch in flight per slot. A batch is 1000 records, so a higher
      // prefetch does not add throughput, it just holds whole batches in memory
      // and delays the redelivery of anything that has to be retried.
      await channel.prefetch(this.config.prefetch);
      await channel.consume(this.config.queue, (message) => {
        if (message) void this.handle(message, channel);
      });
      this.channels.push(channel);
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await Promise.all(
      this.channels.map(async (channel) => {
        try {
          await channel.close();
        } catch {
          // shutting down
        }
      }),
    );
    this.channels = [];
  }

  /**
   * Handles one message. Exposed so tests can drive it without a live consumer.
   *
   * The channel is the one the message arrived on, and is what the ack goes back
   * on. Acking on a different channel is a protocol error, and with several
   * consumers in flight a shared reference would ack the wrong delivery.
   */
  async handle(
    message: ConsumeMessage,
    channel?: Awaited<ReturnType<RabbitService['consumerChannel']>>,
  ): Promise<BatchResult> {
    const ackOn = channel ?? this.channels[0];
    let batch: BatchMessage;
    try {
      batch = JSON.parse(message.content.toString()) as BatchMessage;
    } catch {
      // Unparseable will never succeed on a retry, so it goes straight to the DLQ.
      await this.deadLetter(message);
      return { disposition: 'dead', moved: 0, failed: 0, attempts: 0 };
    }

    let result: BatchResult;
    try {
      result = await this.repository.processBatch(batch.workspaceId, batch.jobId, batch.batchNo);
    } catch {
      // Infrastructure failure, nothing was touched. Retried without spending
      // the attempt budget: a database outage should not be what dead-letters a
      // batch. The reason this is safe is that the lock is released and the
      // items are still pending, so a redelivery starts cleanly.
      result = { disposition: 'retry', moved: 0, failed: 0, attempts: 0 };
    }

    if (result.disposition === 'retry' || result.disposition === 'dead') {
      await this.republish(message, result);
    }
    // Acked last, and only once the outcome is durable. If this process dies
    // before the ack, RabbitMQ redelivers, and the claim is a compare-and-set so
    // the redelivery is harmless.
    ackOn?.ack(message);
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
