import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { OutboxRepository } from './outbox.repository';
import { RabbitBatchPublisher } from './rabbit-batch.publisher';
import { DatabaseService } from '../../shared/database/database.service';
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
 * database either. A batch that fails to publish keeps its place at the back of
 * the queue, so it is retried last instead of ahead of work never tried.
 *
 * Safe to run in more than one replica. A pass claims its rows with FOR UPDATE
 * SKIP LOCKED and holds the lock until it has published or given up, so two relays
 * get disjoint batches instead of both publishing the same ones.
 */
@Injectable()
export class OutboxRelay implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelay.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;
  /** A pass was asked for while one was already running. */
  private again = false;
  /** Last totals written to the log, so an idle relay stays silent. */
  private reported = { published: 0, failed: 0 };
  readonly stats: RelayStats = { published: 0, failed: 0 };

  constructor(
    private readonly outbox: OutboxRepository,
    private readonly publisher: RabbitBatchPublisher,
    private readonly db: DatabaseService,
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

  /**
   * One pass over the unpublished rows.
   *
   * Exposed so tests can drive a pass instead of waiting for the timer, and so a
   * caller that has just written a row - `retry-failed` - can ask for it now
   * rather than at the next tick.
   *
   * A call that arrives while a pass is running does not run a second pass in
   * parallel; it sets a flag and returns, and the running pass loops again. That
   * is not the same as ignoring it. Returning without a flag would drop the
   * request, and the row would sit unpublished until the timer came round again,
   * which is the delay the caller asked us to skip.
   */
  async tick(): Promise<void> {
    if (this.stopped) return;
    if (this.running) {
      this.again = true;
      return;
    }
    this.running = true;
    try {
      // One transaction for the whole pass. The claim takes a row lock that has to
      // survive until published_at is stamped, and that is also what stops a
      // second replica claiming the same batches - see OutboxRepository.claim.
      //
      // The cost is a pooled connection held for the length of the pass, and for
      // publishes that means the broker round trips too. That is the trade for not
      // publishing everything twice, and it is bounded by relayBatchSize.
      await this.db.transaction(async (client) => {
        const abandoned = await this.outbox.abandonExhausted(client, this.config.relayMaxAttempts);
        if (abandoned > 0) {
          this.logger.error(
            `gave up on ${abandoned} batch(es) after ${this.config.relayMaxAttempts} publish ` +
              `attempts; their records are not moved and retry-failed can re-queue them`,
          );
        }

        const rows = await this.outbox.claim(client, this.config.relayBatchSize);
        for (const row of rows) {
          if (this.stopped) return;
          try {
            await this.publisher.publish({
              workspaceId: row.workspace_id,
              jobId: row.job_id,
              batchNo: row.batch_no,
              // From cardinality in the claim query, not item_ids.length: the ids
              // are not selected any more, and were a thousand uuids per batch to
              // count what the database already knew.
              itemCount: row.item_count,
            });
            await this.outbox.markPublished(client, row.id);
            this.stats.published += 1;
          } catch (error) {
            // Leave the row unpublished and stop the pass. Trying the next row
            // would just fail the same way and turn a broker outage into a burst
            // of doomed writes.
            //
            // It is not stranded, though: the attempt count moves, and the claim
            // orders by attempts, so the next pass puts fresh work ahead of this
            // and retries it last rather than at the head of its own queue.
            //
            // Logged because this is the failure the outbox exists to survive, and
            // it is otherwise invisible: the row stays unpublished, so every column
            // still reads exactly as it did before the outage, and once the broker
            // comes back the pass succeeds and nothing anywhere records the gap.
            await this.outbox.markFailed(client, row.id);
            this.stats.failed += 1;
            this.logger.warn(
              `publish failed for job=${row.job_id.slice(0, 8)} batch=${row.batch_no} ` +
                `(attempt ${row.attempts + 1}), stopping this pass: ${(error as Error).message}`,
            );
            return;
          }
        }
      });
    } catch (error) {
      // Database unreachable; the next tick retries. Logged rather than
      // swallowed, because an unreadable outbox is indistinguishable from an
      // empty one - no batches publish, and every batch row still reads as
      // "written and waiting", which looks like a healthy build.
      this.logger.error(`outbox pass failed: ${(error as Error).message}`);
    } finally {
      this.reportProgress();
      this.running = false;
      // Someone asked for another pass while this one was in flight. Looping
      // here rather than on the next timer is what makes the flag worth setting;
      // `again` is cleared first so a request that lands during *this* pass is
      // not also served by it.
      if (this.again && !this.stopped) {
        this.again = false;
        await this.tick();
      }
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
