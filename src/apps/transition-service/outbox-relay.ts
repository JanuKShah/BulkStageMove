import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
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
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;
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
            itemCount: row.item_count,
          });
          await this.outbox.markPublished(row.id);
          this.stats.published += 1;
        } catch {
          // Leave the row unpublished and stop the pass. Trying the next row
          // would just fail the same way and turn a broker outage into a burst
          // of doomed writes.
          await this.outbox.markFailed(row.id);
          this.stats.failed += 1;
          return;
        }
      }
    } catch {
      // Database unreachable; the next tick retries.
    } finally {
      this.running = false;
    }
  }
}
