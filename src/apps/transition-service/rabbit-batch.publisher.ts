import { Injectable } from '@nestjs/common';
import { RabbitService } from '../../shared/rabbit/rabbit.service';
import type { BatchMessage } from '../../shared/rabbit/rabbit.config';

/**
 * Publishes a batch and resolves only once the broker has confirmed it.
 *
 * `waitForConfirms` is what makes the outbox safe to mark rows published. A
 * publish that has merely been written to a socket can still be refused, and
 * marking it sent then would strand the batch with no record of it anywhere.
 */
@Injectable()
export class RabbitBatchPublisher {
  constructor(private readonly rabbit: RabbitService) {}

  async publish(message: BatchMessage): Promise<void> {
    const channel = await this.rabbit.publisherChannel();
    const accepted = channel.publish(
      this.rabbit.config.exchange,
      this.rabbit.config.routingKey,
      Buffer.from(JSON.stringify(message)),
      {
        // Non-persistent messages are lost on a broker restart, which for a
        // 50,000-row job means work that was accepted and then silently gone.
        persistent: true,
        contentType: 'application/json',
        messageId: `${message.jobId}:${message.batchNo}`,
      },
    );
    if (!accepted) {
      // The channel's buffer is full; wait for drain before confirming.
      await new Promise<void>((resolve) => channel.once('drain', () => resolve()));
    }
    await channel.waitForConfirms();
  }
}
