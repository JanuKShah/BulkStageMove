import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { OutboxRepository } from './outbox.repository';
import { RabbitBatchPublisher } from './rabbit-batch.publisher';
import { RABBIT_CONFIG, type RabbitConfig } from '../../shared/rabbit/rabbit.config';

export interface RelayStats {
  published: number;
  failed: number;
}

/**
 * Drains the outbox into the broker.
 *
 * A row is only marked published after the broker confirms, so a crash between
 * the two re-publishes rather than loses. That makes delivery at-least-once,
 * which is the correct trade here: duplicate batches are harmless because the
 * worker's claim is a compare-and-set on status='pending', whereas a lost batch
 * would strand items and hang the job at pending forever.
 *
 * The loop stops on a broker error rather than spinning, and resumes on the next
 * tick. A broker that is down should not turn into a hot retry loop against the
 * database either.
 */
@Injectable()
export class OutboxRelay implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;
  /** Last totals written to the log, so an idle relay stays silent. */
  private reported = { published: 0, failed: 0 };
  readonly stats: RelayStats = { published: 0, failed: 0 };

  constructor(
    private readonly outbox: OutboxRepository,
    private readonly publisher: RabbitBatchPublisher,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  onModuleInit(): void {
    this.stopped = false;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.config.relayIntervalMs);
    this.timer.unref();
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** Exposed so tests can drive one pass instead of waiting for the timer. */
  async tick(): Promise<void> {
    // Overlapping ticks would publish the same row twice before either marks it.
    if (this.running || this.stopped) return;
    this.running = true;
    try {
      const rows = await this.outbox.unpublished(this.config.relayBatchSize);
      for (const row of rows) {
        if (this.stopped) return;
        try {
          await this.publisher.publish({
            workspaceId: row.workspace_id,
            jobId: row.job_id,
            batchNo: row.batch_no,
            itemCount: row.item_ids.length,
          });
          await this.outbox.markPublished(row.id);
          this.stats.published += 1;
        } catch (error) {
          // Leave the row unpublished and stop the pass. Trying the next row
          // would just fail the same way and turn a broker outage into a burst
          // of doomed writes.
          //
          // Logged because this is the failure the outbox exists to survive, and
          // it is otherwise invisible: the row stays unpublished, so every
          // column still reads exactly as it did before the outage, and once the
          // broker comes back the pass succeeds and nothing anywhere records that
          // there was a gap. The batch is not lost - the job just sits in
          // preparing for as long as the broker is down.
          await this.outbox.markFailed(row.id);
          this.stats.failed += 1;
          this.logger.warn(
            `publish failed for job=${row.job_id.slice(0, 8)} batch=${row.batch_no}, ` +
              `stopping this pass: ${(error as Error).message}`,
          );
          return;
        }
      }
    } catch (error) {
      // Database unreachable; the next tick retries. Logged rather than
      // swallowed, because an unreadable outbox is indistinguishable from an
      // empty one - no batches publish, and every batch row still reads as
      // "written and waiting", which looks like a healthy build.
      this.logger.error(`outbox pass failed: ${(error as Error).message}`);
    } finally {
      this.reportProgress();
      this.running = false;
    }
  }

  /**
   * Writes the running totals, but only when they have moved.
   *
   * The relay ticks on a timer whether or not there is anything to do, so
   * logging unconditionally would bury the one line that matters - the pass where
   * publishing actually failed - under a stream of zeroes. Comparing against the
   * last reported pair means a healthy idle relay says nothing and a backlog
   * being worked through says so continuously, which is the signal that the
   * snapshot build is outrunning the relay.
   */
  private reportProgress(): void {
    if (
      this.stats.published === this.reported.published &&
      this.stats.failed === this.reported.failed
    ) {
      return;
    }
    this.logger.log(
      `relayed ${this.stats.published} batch(es), ${this.stats.failed} failed since start`,
    );
    this.reported = { ...this.stats };
  }
}
