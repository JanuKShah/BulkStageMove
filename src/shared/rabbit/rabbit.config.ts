/**
 * Broker settings, read once at startup and validated.
 *
 * Defaults are the values this project runs with, so `docker compose up` works
 * with no broker configuration at all. Anything supplied is the override.
 *
 * Two of these are correctness rather than tuning. Publisher confirms are not
 * optional: without them a publish can be silently dropped and the job sits at
 * pending with no error anywhere. And prefetch stays at 1 because a batch is
 * 1000 records - a higher value does not increase throughput, it just holds
 * several whole batches in the consumer's memory.
 */

/** Injection token. Provided by a factory, because it is read from the env. */
export const RABBIT_CONFIG = 'RABBIT_CONFIG';

export interface RabbitConfig {
  url: string;
  exchange: string;
  routingKey: string;
  queue: string;
  retryQueue: string;
  deadLetterQueue: string;
  prefetch: number;
  maxAttempts: number;
  /** Per-attempt backoff, index 0 used after the first failure. */
  retryDelaysMs: number[];
  deadLetterTtlMs: number;
  deadLetterMaxLength: number;
  heartbeatSeconds: number;
  consumerConcurrency: number;
  /** How often the outbox relay looks for unpublished batches. */
  relayIntervalMs: number;
  relayBatchSize: number;
}

const DEFAULTS: RabbitConfig = {
  url: 'amqp://app:app_dev_password@localhost:5672/%2f',
  exchange: 'bulk.move',
  routingKey: 'batch',
  queue: 'bulk.move',
  retryQueue: 'bulk.move.retry',
  deadLetterQueue: 'bulk.move.dlq',
  prefetch: 1,
  maxAttempts: 3,
  retryDelaysMs: [1_000, 5_000, 30_000],
  deadLetterTtlMs: 7 * 24 * 60 * 60 * 1_000,
  deadLetterMaxLength: 10_000,
  heartbeatSeconds: 30,
  consumerConcurrency: 4,
  relayIntervalMs: 250,
  relayBatchSize: 50,
};

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got "${raw}"`);
  }
  return parsed;
}

function list(name: string, fallback: number[]): number[] {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const parts = raw
    .split(',')
    .map((p) => Number(p.trim()))
    .filter((n) => Number.isFinite(n) && n >= 0);
  if (parts.length === 0) throw new Error(`${name} must be a comma-separated list of delays`);
  return parts;
}

export function rabbitConfig(): RabbitConfig {
  return {
    url: process.env.RABBITMQ_URL ?? DEFAULTS.url,
    exchange: process.env.RABBITMQ_EXCHANGE ?? DEFAULTS.exchange,
    routingKey: process.env.RABBITMQ_ROUTING_KEY ?? DEFAULTS.routingKey,
    queue: process.env.RABBITMQ_QUEUE ?? DEFAULTS.queue,
    retryQueue: process.env.RABBITMQ_RETRY_QUEUE ?? DEFAULTS.retryQueue,
    deadLetterQueue: process.env.RABBITMQ_DLQ ?? DEFAULTS.deadLetterQueue,
    prefetch: int('RABBITMQ_PREFETCH', DEFAULTS.prefetch),
    maxAttempts: int('RABBITMQ_MAX_ATTEMPTS', DEFAULTS.maxAttempts),
    retryDelaysMs: list('RABBITMQ_RETRY_DELAYS_MS', DEFAULTS.retryDelaysMs),
    deadLetterTtlMs: int('RABBITMQ_DLQ_TTL_MS', DEFAULTS.deadLetterTtlMs),
    deadLetterMaxLength: int('RABBITMQ_DLQ_MAX_LENGTH', DEFAULTS.deadLetterMaxLength),
    heartbeatSeconds: int('RABBITMQ_HEARTBEAT', DEFAULTS.heartbeatSeconds),
    consumerConcurrency: int('RABBITMQ_CONSUMER_CONCURRENCY', DEFAULTS.consumerConcurrency),
    relayIntervalMs: int('RABBITMQ_RELAY_INTERVAL_MS', DEFAULTS.relayIntervalMs),
    relayBatchSize: int('RABBITMQ_RELAY_BATCH_SIZE', DEFAULTS.relayBatchSize),
  };
}

/** The batch a worker receives. Names a page of items rather than carrying it. */
export interface BatchMessage {
  workspaceId: string;
  jobId: string;
  batchNo: number;
  itemCount: number;
}
