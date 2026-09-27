import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import type { ConsumeMessage } from 'amqplib';
import { RabbitService } from '../../shared/rabbit/rabbit.service';
import { ms1 } from '../../shared/observability/timing';
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
 *
 * It also owns the per-batch log line, because it is the only place that knows
 * which consumer slot took the message. That slot is the piece that makes a
 * throughput number diagnosable: with twelve consumers, "eleven idle and one
 * slow" and "all twelve evenly loaded" produce identical totals and opposite
 * conclusions, and nothing else recorded here can tell them apart.
 */
@Injectable()
export class BatchWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(BatchWorker.name);
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
      // The slot is captured here rather than derived later, so it names the
      // channel the message actually arrived on.
      await channel.consume(this.config.queue, (message) => {
        if (message) void this.handle(message, channel, i);
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
   *
   * The slot defaults to 0 because the test harness calls this directly with no
   * consumer in the picture; the number only means something where there is one.
   */
  async handle(
    message: ConsumeMessage,
    channel?: Awaited<ReturnType<RabbitService['consumerChannel']>>,
    slot = 0,
  ): Promise<BatchResult> {
    const ackOn = channel ?? this.channels[0];
    let batch: BatchMessage;
    try {
      batch = JSON.parse(message.content.toString()) as BatchMessage;
    } catch {
      // Unparseable will never succeed on a retry, so it goes straight to the DLQ.
      await this.deadLetter(message);
      this.logger.warn(`slot=${slot} unparseable message dead-lettered`);
      return { disposition: 'dead', moved: 0, failed: 0, skipped: 0, attempts: 0 };
    }

    let result: BatchResult;
    try {
      result = await this.repository.processBatch(batch.workspaceId, batch.jobId, batch.batchNo);
    } catch (error) {
      // Infrastructure failure, nothing was touched. Retried without spending
      // the attempt budget: a database outage should not be what dead-letters a
      // batch. The reason this is safe is that the lock is released and the
      // items are still pending, so a redelivery starts cleanly.
      //
      // Logged rather than swallowed, because this is the path where the work
      // stops making progress and nothing else records it: the batch row is
      // untouched, so no column shows the failure either.
      this.logger.error(
        `job=${batch.jobId.slice(0, 8)} batch=${batch.batchNo} slot=${slot} ` +
          `infrastructure failure, republishing: ${(error as Error).message}`,
      );
      result = { disposition: 'retry', moved: 0, failed: 0, skipped: 0, attempts: 0 };
    }

    this.logBatch(batch, result, slot);

    if (result.disposition === 'retry' || result.disposition === 'dead') {
      await this.republish(message, result);
    }
    // Acked last, and only once the outcome is durable. If this process dies
    // before the ack, RabbitMQ redelivers, and the claim is a compare-and-set so
    // the redelivery is harmless.
    ackOn?.ack(message);
    return result;
  }

  /**
   * One line per batch.
   *
   * A bulk move is fifty of these at 50,000 records and five hundred at 500,000,
   * which is nothing for a system that is idle between jobs. Everything here is
   * something the database cannot report: the pool pressure is invisible because
   * started_at is stamped after the claim, and the slot is invisible because
   * nothing records which consumer took what.
   *
   * The job id is truncated to eight characters. It is only ever read by eye
   * against a job row, and the full uuid doubles the line length to carry
   * characters nobody scans.
   */
  private logBatch(batch: BatchMessage, result: BatchResult, slot: number): void {
    const t = result.timing;
    const timing = t
      ? `wait=${ms1(t.poolWaitMs)} work=${ms1(t.workMs)} ` +
        `pool=${t.poolSize}conn/${t.poolWaiting}queued`
      : 'wait=- work=- pool=-';
    this.logger.log(
      `job=${batch.jobId.slice(0, 8)} batch=${batch.batchNo} slot=${slot} ` +
        `${result.disposition} moved=${result.moved} failed=${result.failed} ` +
        `skipped=${result.skipped} attempts=${result.attempts} ${timing}`,
    );
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
