import { Inject, Injectable, type OnModuleDestroy } from '@nestjs/common';
import amqp, { type Channel, type ChannelModel, type ConfirmChannel } from 'amqplib';
import { RABBIT_CONFIG, type RabbitConfig } from './rabbit.config';

/** amqplib takes the heartbeat as a URL parameter, not as a socket option. */
function withHeartbeat(url: string, seconds: number): string {
  if (seconds <= 0) return url;
  return url.includes('?') ? `${url}&heartbeat=${seconds}` : `${url}?heartbeat=${seconds}`;
}

/**
 * Owns the broker connection, a publisher channel and consumer channels.
 *
 * Publisher confirms are not optional. Without them a publish is a local write
 * to a socket buffer, and a broker that refuses or drops it leaves the job at
 * pending with no error on either side. With a confirm channel the promise
 * settles only once the broker has taken responsibility for the message, which
 * is what lets the relay mark an outbox row published.
 *
 * Connection loss is not fatal. The channel is discarded on close and recreated
 * on next use, so a broker restart heals without restarting the process.
 */
@Injectable()
export class RabbitService implements OnModuleDestroy {
  private connection: ChannelModel | null = null;
  private publisher: ConfirmChannel | null = null;
  private connecting: Promise<ChannelModel> | null = null;

  constructor(@Inject(RABBIT_CONFIG) readonly config: RabbitConfig) {}

  private async connect(): Promise<ChannelModel> {
    if (this.connection) return this.connection;
    // Concurrent callers must not open competing connections during a retry.
    this.connecting ??= amqp
      .connect(withHeartbeat(this.config.url, this.config.heartbeatSeconds))
      .then((connection) => {
        this.connection = connection;
        this.connecting = null;
        connection.on('error', () => this.reset());
        connection.on('close', () => this.reset());
        return connection;
      })
      .catch((error: unknown) => {
        this.connecting = null;
        throw error;
      });
    return this.connecting;
  }

  private reset(): void {
    this.connection = null;
    this.publisher = null;
    this.connecting = null;
  }

  async publisherChannel(): Promise<ConfirmChannel> {
    if (this.publisher) return this.publisher;
    const connection = await this.connect();
    this.publisher = await connection.createConfirmChannel();
    this.publisher.on('error', () => {
      this.publisher = null;
    });
    this.publisher.on('close', () => {
      this.publisher = null;
    });
    return this.publisher;
  }

  /** A separate channel for consuming; a channel in confirm mode cannot consume. */
  async consumerChannel(): Promise<Channel> {
    const connection = await this.connect();
    return connection.createChannel();
  }

  /**
   * Declares the topology. Idempotent, so every process can call it on startup
   * and it is the only place the layout is written down.
   *
   * One exchange and one queue. There is no dead letter queue: a batch that runs
   * out of attempts is marked failed in Postgres - `bulk_job_outbox.status =
   * 'failed'` with the reason, plus a `bulk_job_failure` row per record - and the
   * message is acked. A queue would only have repeated what the batch row already
   * says, and it could not be queried. There are no retry queues either; a
   * requeued message comes straight back, and the attempt count is the batch
   * row's own column.
   */
  async declareTopology(channel: Channel): Promise<void> {
    const c = this.config;
    await channel.assertExchange(c.exchange, 'direct', { durable: true });
    await channel.assertQueue(c.queue, { durable: true });
    await channel.bindQueue(c.queue, c.exchange, c.routingKey);
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.publisher?.close();
      await this.connection?.close();
    } catch {
      // shutting down; a close failure here is not actionable
    }
  }
}
