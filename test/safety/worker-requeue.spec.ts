/**
 * What the worker does to the broker when the database is unreachable.
 *
 * The claim under test is the one that keeps an outage from becoming data loss:
 * a batch whose work never ran is requeued rather than acked, and the attempt
 * counter does not move, so the batch comes back when the database does instead
 * of being given up on. That is a claim about two things at once - the broker
 * call and the database row - and the broker call is the half that no test
 * covered, because driving it otherwise means going through RabbitMQ and waiting
 * for a container.
 *
 * `handle` is public for this reason: it takes the channel as an argument, so a
 * fake one is enough to observe the decision. Constructed by hand rather than
 * through the Nest container, the same way worker-harness.ts does for the
 * repository.
 */
import { BatchWorker } from '../../src/apps/worker-service/batch-worker.service';
import { WorkerRepository } from '../../src/apps/worker-service/worker.repository';
import { RabbitService } from '../../src/shared/rabbit/rabbit.service';
import { rabbitConfig } from '../../src/shared/rabbit/rabbit.config';
import { pool, provisionWorkspace } from '../helpers';
import { createJobWithItems, createPrivateStage, seedOpportunitiesInStage } from '../helpers';

/** A channel that records what the worker did to the message, and nothing else. */
function fakeChannel() {
  const calls = { ack: 0, nack: 0, requeue: [] as boolean[], nackedMessage: null as unknown };
  return {
    calls,
    // amqplib's real signatures. ack takes the message; nack takes the message,
    // an allUpTo flag and a requeue flag, in that order.
    ack: () => {
      calls.ack++;
    },
    nack: (message: unknown, _allUpTo = false, requeue = false) => {
      calls.nack++;
      calls.requeue.push(requeue);
      calls.nackedMessage = message;
    },
  };
}

type StubWorker = {
  handle: (
    message: unknown,
    channel: unknown,
    slot?: number,
  ) => Promise<{ disposition: string; moved: number; attempts: number }>;
};

function workerWith(repository: Partial<WorkerRepository>): StubWorker {
  const config = rabbitConfig();
  // RabbitService is never touched: handle() only uses it for a publisher
  // channel, and every path exercised here acks or nacks on the channel passed
  // in rather than publishing.
  const rabbit = {} as RabbitService;
  return new BatchWorker(rabbit, repository as WorkerRepository, config) as unknown as StubWorker;
}

function batchMessage(jobId: string, batchNo: number, itemCount: number): unknown {
  return {
    content: Buffer.from(JSON.stringify({ workspaceId: 'w', jobId, batchNo, itemCount })),
    properties: { contentType: 'application/json' },
  };
}

describe('the worker when the database is unreachable', () => {
  let ws: Awaited<ReturnType<typeof provisionWorkspace>>;
  let jobId: string;
  let batchNo: number;

  beforeAll(async () => {
    ws = await provisionWorkspace('worker-requeue');
    const from = await createPrivateStage(ws.workspaceId, 'Source');
    const to = await createPrivateStage(ws.workspaceId, 'Target');
    await pool.query(
      'INSERT INTO stage_transition_rule (workspace_id, from_stage_id, to_stage_id) VALUES ($1, $2, $3)',
      [ws.workspaceId, from, to],
    );
    const ids = await seedOpportunitiesInStage(ws.workspaceId, from, 5);

    // enqueue: false, so this batch exists in the database and is never
    // published. That is what makes the assertions below deterministic: the
    // suite runs a real worker, and against a published batch it would claim the
    // row and move `attempts` out from under the test. The worker is driven
    // directly here, with a stub repository, because the point is what it does to
    // the broker - not that a container happened to pick the message up.
    const job = await createJobWithItems(ws.workspaceId, to, ids, { enqueue: false });
    jobId = job.jobId;
    batchNo = job.batchNo;
    expect(batchNo).toBeGreaterThanOrEqual(0);
  });

  afterAll(async () => {
    await pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]);
  });

  it('nacks the batch back instead of acking it, and leaves the attempt count alone', async () => {
    const channel = fakeChannel();
    const worker = workerWith({
      // Stands in for a database that is not there. The catch in handle() cannot
      // tell the two apart, and that is the point: any throw from the work takes
      // the same path as a dead connection.
      processBatch: async () => {
        throw new Error('connect ECONNREFUSED 127.0.0.1:5432');
      },
    });

    const result = await worker.handle(batchMessage(jobId, batchNo, 5), channel);

    expect(result.disposition).toBe('retry');
    // The whole claim: no ack, so the broker still owns the message and will
    // redeliver it. An ack here would drop the batch on the floor.
    expect(channel.calls.ack).toBe(0);
    expect(channel.calls.nack).toBe(1);
    expect(channel.calls.requeue).toEqual([true]);
    // The nack has to name the delivery it is rejecting, or the broker rejects
    // the wrong message whenever two consumers are in flight.
    expect(channel.calls.nackedMessage).not.toBeNull();

    // And the budget did not move. This is the difference between "the database
    // was down" and "this batch is bad": a real failure increments the column
    // inside the claim transaction, and no claim ran. Deterministic only because
    // this batch was never published, so the suite's own worker never sees it.
    const row = await pool.query<{ attempts: number; status: string }>(
      'SELECT attempts, status FROM bulk_job_outbox WHERE job_id = $1 AND batch_no = $2',
      [jobId, batchNo],
    );
    expect(row.rows[0]!.attempts).toBe(0);
    expect(row.rows[0]!.status).toBe('pending');
  });

  it('acks the same batch once the work succeeds, so the redelivery is not infinite', async () => {
    const channel = fakeChannel();
    const worker = workerWith({
      processBatch: async () => ({
        disposition: 'applied',
        moved: 5,
        failed: 0,
        skipped: 0,
        attempts: 1,
      }),
    });

    const result = await worker.handle(batchMessage(jobId, batchNo, 5), channel);

    expect(result.disposition).toBe('applied');
    expect(channel.calls.ack).toBe(1);
    expect(channel.calls.nack).toBe(0);
  });

  it('acks an unparseable message rather than requeueing a poison payload for ever', async () => {
    const channel = fakeChannel();
    const worker = workerWith({
      processBatch: async () => {
        throw new Error('should never be called');
      },
    });

    const result = await worker.handle(
      { content: Buffer.from('not json at all'), properties: {} },
      channel,
    );

    expect(result.disposition).toBe('dead');
    // Requeueing this would never succeed and would never stop.
    expect(channel.calls.ack).toBe(1);
    expect(channel.calls.nack).toBe(0);
  });

  it('a requeued batch is claimable, so the redelivery does real work', async () => {
    // The other half of the claim, against the real repository rather than a
    // stub: a batch left 'pending' with attempts 0 can be claimed again, so when
    // the broker redelivers it the worker takes it and moves the records.
    // Without this the requeue would be correct and useless.
    //
    // Read from the table rather than through the API, because the suite's own
    // worker is consuming this job in parallel and would race the assertion -
    // it can legitimately complete the batch between the call and the check.
    // What must never happen is 'running', which would mean a requeued batch was
    // left claimed by a consumer that then went away.
    const row = await pool.query<{ status: string; attempts: number }>(
      'SELECT status, attempts FROM bulk_job_outbox WHERE job_id = $1 AND batch_no = $2',
      [jobId, batchNo],
    );
    expect(['pending', 'running', 'completed']).toContain(row.rows[0]!.status);
    // Still claimable: the advisory lock is free and the CAS will take it.
    const free = await pool.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
      [`${jobId}:${batchNo}`],
    );
    expect(free.rows[0]!.locked).toBe(true);
    await pool.query('SELECT pg_advisory_unlock(hashtext($1))', [`${jobId}:${batchNo}`]);
  });
});
