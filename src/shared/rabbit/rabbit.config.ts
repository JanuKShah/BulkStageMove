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
  prefetch: number;
  /**
   * How many times a batch is attempted before it is marked failed for good.
   *
   * Counted by the application, in the batch row's own `attempts` column, which
   * is incremented by the same transaction that claims the batch. The database is
   * the counter because the database is also where the failure is recorded, so
   * the number that decides and the row that explains it cannot disagree.
   */
  maxAttempts: number;
  heartbeatSeconds: number;
  consumerConcurrency: number;
  /**
   * How often the transition service looks for jobs whose snapshot is unfinished.
   *
   * It is the only thing that drives the build now - submission returns before
   * the batches exist - so this is the latency between a job being accepted and
   * its first batch being written, not a background nicety.
   */
  snapshotSweepIntervalMs: number;
  /**
   * How many unfinished jobs one sweep looks at.
   *
   * This, not the replica count, is what bounds how many jobs can be built at
   * once. A sweep walks its candidates in order and skips the ones another
   * process already holds, so N replicas spread across N of them - but only if
   * the limit is at least N. At the default of 25, five replicas find five
   * distinct jobs; leave it at 5 and a sixth replica has nothing left to claim
   * and idles.
   *
   * Sized above the expected replica count rather than equal to it, because a
   * candidate that is mid-build stays 'preparing' for the whole walk. Too tight
   * and every replica spends its pass skipping jobs the others are already on.
   */
  snapshotSweepLimit: number;
  /** How often the outbox relay looks for unpublished batches. */
  relayIntervalMs: number;
  relayBatchSize: number;
}

const DEFAULTS: RabbitConfig = {
  url: 'amqp://app:app_dev_password@localhost:5672/%2f',
  exchange: 'bulk.move',
  routingKey: 'batch',
  queue: 'bulk.move',
  prefetch: 1,
  maxAttempts: 3,
  heartbeatSeconds: 30,
  consumerConcurrency: 12,
  // 125 ms rather than 250, halved to cut the floor on a small job. A job waits
  // one sweep tick before its first batch exists and one relay tick before any of
  // it is published, so the expected wait is the sum of the two, and that sum is
  // most of the wall clock below ~15,000 records - measured 386-470 ms to settle
  // 5,000 records, against under 100 ms of actual work.
  //
  // It is a mitigation, not a fix: polling still imposes a wait, and halving the
  // interval only halves it. What makes the tighter tick cheap is that both polls
  // are index scans that usually return nothing - the sweep reads
  // bulk_job_preparing_idx and the relay reads bulk_job_outbox_unpublished_idx,
  // itself a partial index over `WHERE published_at IS NULL`, so a tick with no
  // work behind it touches one index entry and nothing else. Going below this
  // would need a real wake-up rather than a shorter wait, since the cost stops
  // being free somewhere below the tick itself.
  snapshotSweepIntervalMs: 125,
  snapshotSweepLimit: 25,
  relayIntervalMs: 125,
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
    prefetch: int('RABBITMQ_PREFETCH', DEFAULTS.prefetch),
    maxAttempts: int('RABBITMQ_MAX_ATTEMPTS', DEFAULTS.maxAttempts),
    heartbeatSeconds: int('RABBITMQ_HEARTBEAT', DEFAULTS.heartbeatSeconds),
    consumerConcurrency: int('RABBITMQ_CONSUMER_CONCURRENCY', DEFAULTS.consumerConcurrency),
    snapshotSweepIntervalMs: int('SNAPSHOT_SWEEP_INTERVAL_MS', DEFAULTS.snapshotSweepIntervalMs),
    snapshotSweepLimit: int('SNAPSHOT_SWEEP_LIMIT', DEFAULTS.snapshotSweepLimit),
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
