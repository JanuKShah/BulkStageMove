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
 * turns its answer into broker behaviour: a retry is nacked straight back onto the
 * queue, and a give-up is acked, because processBatch has already written the
 * failure to the batch row. There is no delay and no second queue, so the count
 * that eventually stops a retry is the batch row's own `attempts` column.
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
      // Unparseable will never succeed on a retry. There is no batch row to mark
      // - the message never parsed into a job and batch number - so this is the
      // one failure the database cannot record, and it is logged rather than
      // silently acked. Acked because requeueing an unparseable message is an
      // infinite loop against a poison payload.
      this.logger.error(
        `slot=${slot} unparseable batch message, dropped: ${message.content.toString('utf8').slice(0, 200)}`,
      );
      ackOn?.ack(message);
      return { disposition: 'dead', moved: 0, failed: 0, skipped: 0, attempts: 0 };
    }

    let result: BatchResult;
    try {
      result = await this.repository.processBatch(batch.workspaceId, batch.jobId, batch.batchNo);
    } catch (error) {
      // Infrastructure failure, nothing was touched: the lock is released and the
      // items are still pending, so a redelivery starts cleanly.
      //
      // Requeued rather than failed, because the batch is fine and the database
      // is not. Nothing was written, so the attempt column did not move either and
      // a long outage retries indefinitely rather than marking the batch failed.
      //
      // Logged rather than swallowed, because this is the path where the work
      // stops making progress and nothing else records it: the batch row is
      // untouched, so no column shows the failure either.
      this.logger.error(
        `job=${batch.jobId.slice(0, 8)} batch=${batch.batchNo} slot=${slot} ` +
          `infrastructure failure, requeueing: ${(error as Error).message}`,
      );
      result = { disposition: 'retry', moved: 0, failed: 0, skipped: 0, attempts: 0 };
    }

    this.logBatch(batch, result, slot);

    // Exactly one of these runs. A delivery is settled once: acking a message
    // that has already been nacked is a double-settle, and amqplib closes the
    // channel on it, so the redelivery the nack asked for would never arrive.
    if (result.disposition === 'retry') {
      this.reject(message, ackOn);
    } else {
      // 'applied', 'skipped' and 'dead' all settle here. 'dead' needs nothing
      // more: processBatch already wrote the failure to bulk_job_outbox and
      // bulk_job_failure and settled the job. Acked only once the outcome is
      // durable, so a process that dies first leaves the message unacked and the
      // broker redelivers it - which is harmless, because the claim is a
      // compare-and-set and a completed batch is recognised rather than reapplied.
      ackOn?.ack(message);
    }
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

  /**
   * Hand the message back for another attempt.
   *
   * The channel is the one the delivery arrived on, because that is the only one
   * the broker will accept a nack on. A shared reference would nack the wrong
   * delivery whenever two consumers are in flight.
   *
   * No channel means the connection is already gone, and an unacked delivery is
   * redelivered by the broker when it notices. So there is nothing to repair
   * here, and nacking on a guessed channel would be the error.
   */
  private reject(
    message: ConsumeMessage,
    channel: Awaited<ReturnType<RabbitService['consumerChannel']>> | undefined,
  ): void {
    channel?.nack(message, false, true);
  }

  get isStopping(): boolean {
    return this.stopping;
  }
}
