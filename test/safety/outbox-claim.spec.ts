/**
 * What the relay's claim guarantees, and what it stops.
 *
 * Three separate problems, one query:
 *
 *   1. Two relay replicas read the same unpublished rows and both publish them.
 *      Correct only because the worker's claim made the duplicate harmless, and
 *      wasteful in proportion to the replica count.
 *   2. `item_ids` was selected - a thousand uuids per batch, about 37KB - purely
 *      to run `.length` on it in JavaScript.
 *   3. A failed publish left `published_at` NULL and ordered by `created_at`, so
 *      the failing row sat at the head of its own job's queue and was retried
 *      ahead of work that had never been tried. One poisoned batch blocked the
 *      rest.
 *
 * The claim is FOR UPDATE SKIP LOCKED inside a transaction, ordered by attempts
 * then created_at, selecting cardinality rather than the array.
 */
import { randomUUID } from 'node:crypto';
import { OutboxRepository } from '../../src/apps/transition-service/outbox.repository';
import { DatabaseService } from '../../src/shared/database/database.service';
import { pool, provisionWorkspace, createPrivateStage, destroyWorkspace } from '../helpers';

/** Two independent connections, because SKIP LOCKED only means anything across sessions. */
let db: DatabaseService;
let repo: OutboxRepository;

function uuids(n: number): string[] {
  return Array.from({ length: n }, () => randomUUID());
}

/**
 * An attempt count no real row will reach.
 *
 * Used to give a test exclusive ownership of the rows an unscoped query matches,
 * so an assertion on a total is that test's arithmetic and not another suite's
 * leftovers in the same database.
 */
const UNREACHABLE = 1_000_000;

describe('the outbox claim', () => {
  let ws: Awaited<ReturnType<typeof provisionWorkspace>>;
  let target: string;

  beforeAll(async () => {
    process.env.DATABASE_URL ??= 'postgres://app:app_dev_password@localhost:5432/bulk_stage_move';
    db = new DatabaseService();
    repo = new OutboxRepository(db);

    ws = await provisionWorkspace('outbox-claim');
    target = await createPrivateStage(ws.workspaceId, 'Target');
  });

  afterAll(async () => {
    // destroyWorkspace rather than a bare DELETE: this suite leaves row locks
    // around from the disjoint-claims test, and the cascade can deadlock against
    // them. The helper retries on 40P01, which is what the other specs do.
    await destroyWorkspace(ws.workspaceId);
    await db.onModuleDestroy();
    await pool.end();
  });

  /**
   * Creates unpublished batches inside the caller's transaction.
   *
   * Inserting and claiming in one transaction is what makes this suite
   * deterministic. The relay in the running transition-service ticks every 125ms
   * and claims unpublished rows, so anything committed here would be published
   * out from under the assertion - which is exactly what happened when this suite
   * committed its rows first. Uncommitted rows are invisible to every other
   * session, so the relay cannot see them at all.
   */
  async function seedInTransaction(
    client: import('pg').PoolClient,
    sizes: number[],
    attempts: number[] = sizes.map(() => 0),
  ): Promise<string> {
    const job = await client.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id,
                             snapshot_at, status, total_matched)
       VALUES ($1, $2, '{}'::jsonb, $3, now(), 'pending', $4) RETURNING id`,
      [ws.workspaceId, `outbox-claim-${randomUUID()}`, target, sizes.reduce((a, b) => a + b, 0)],
    );
    const id = job.rows[0]!.id;
    for (let i = 0; i < sizes.length; i++) {
      await client.query(
        `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids, attempts)
         VALUES ($1, $2, $3, $4::uuid[], $5)`,
        [ws.workspaceId, id, i, uuids(sizes[i]!), attempts[i]!],
      );
    }
    return id;
  }

  it('returns a count, not a thousand uuids', async () => {
    await db.transaction(async (client) => {
      await seedInTransaction(client, [10, 3]);

      const [row] = await repo.claim(client, 1);
      expect(row).toBeDefined();
      // The whole point of cardinality: the relay learns how many records the
      // batch covers without the ids crossing the wire.
      expect(row!.item_count).toBe(10);
      // And they are genuinely absent, which is what stops the 37KB.
      expect((row as unknown as { item_ids?: unknown }).item_ids).toBeUndefined();
    });
  });

  it('retries a failed publish behind work that has never been tried', async () => {
    // Head-of-line blocking. Before this, ordering was by created_at alone, so a
    // row whose publish failed stayed at the head of its own job's queue and was
    // re-selected ahead of work that had never been tried - one poisoned batch
    // blocking every batch after it.
    await db.transaction(async (client) => {
      // attempts 0, 0, 1 - the 1 is a batch that already failed a publish.
      const id = await seedInTransaction(client, [10, 10, 10], [0, 0, 1]);

      const order = await repo.claim(client, 50);
      const mine = order.filter((r) => r.job_id === id);
      expect(mine).toHaveLength(3);

      // Fresh work first, the retried batch last.
      expect(mine.map((r) => r.attempts)).toEqual([0, 0, 1]);
    });
  });

  it('marks a batch failed once the relay runs out of attempts, and settles its job', async () => {
    // Without this, capping attempts would abandon rows: never published, never
    // removed, records never moved, job stuck at pending with nothing recording
    // why. This is what stops that.
    //
    // The attempt count is set absurdly high rather than to the relay's real cap.
    // abandonExhausted is deliberately unscoped - a relay owns the whole outbox, so
    // it abandons every exhausted row wherever it finds one - which means it also
    // picks up any other workspace's rows in this shared database and adds their
    // size to *their* job. That is right in production and wrong for an exact
    // assertion here: this test would read another suite's leftovers. No real row
    // will ever reach a million attempts, so only this test's rows qualify and the
    // arithmetic below is genuinely this job's.
    const id = await db.transaction(async (client) => {
      const job = await seedInTransaction(client, [10, 4], [UNREACHABLE, UNREACHABLE]);
      const n = await repo.abandonExhausted(client, UNREACHABLE);
      expect(n).toBe(2);

      const rows = await client.query<{
        status: string;
        error: string | null;
        failed_count: number;
      }>(
        'SELECT status, error, failed_count FROM bulk_job_outbox WHERE job_id = $1 ORDER BY batch_no',
        [job],
      );
      for (const r of rows.rows) {
        expect(r.status).toBe('failed');
        expect(r.error).toMatch(/relay gave up/);
        // The records are not moved, so they are counted as failed.
        expect(r.failed_count).toBeGreaterThan(0);
      }

      // The job is not left pending with nothing a worker could take.
      const settled = await client.query<{ status: string; failed_count: number }>(
        'SELECT status, failed_count FROM bulk_job WHERE id = $1',
        [job],
      );
      expect(settled.rows[0]!.status).toBe('failed');
      expect(settled.rows[0]!.failed_count).toBe(14);
      return job;
    });
    expect(id).toBeDefined();
  });

  it('a marked-published batch is never claimed again', async () => {
    await db.transaction(async (client) => {
      const id = await seedInTransaction(client, [10, 10]);
      const claimed = await repo.claim(client, 50);
      for (const r of claimed) await repo.markPublished(client, r.id);

      const again = await repo.claim(client, 50);
      expect(again.filter((r) => r.job_id === id)).toEqual([]);
    });
  });

  it('gives two concurrent claims disjoint rows', async () => {
    // The reason for SKIP LOCKED, and the one test that needs committed rows:
    // two sessions can only both see a row once it is committed, and the live
    // relay claims unpublished rows on a 125ms tick. So the rows are inserted in
    // a single statement and claimed immediately after.

    // One statement, so the rows are committed in a single round trip. One
    // round trip per row would take long enough for the live relay to publish
    // some of them before the claims ran, and this test has to see them all.
    const job = await pool.query<{ id: string }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id,
                             snapshot_at, status, total_matched)
       VALUES ($1, $2, '{}'::jsonb, $3, now(), 'pending', 1200) RETURNING id`,
      [ws.workspaceId, `disjoint-${randomUUID()}`, target],
    );
    const ids = job.rows[0]!.id;
    await pool.query(
      `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
       SELECT $1, $2, g, ARRAY[gen_random_uuid(), gen_random_uuid(),
                               gen_random_uuid(), gen_random_uuid(),
                               gen_random_uuid(), gen_random_uuid(),
                               gen_random_uuid(), gen_random_uuid(),
                               gen_random_uuid(), gen_random_uuid()]
         FROM generate_series(0, 119) g`,
      [ws.workspaceId, ids],
    );

    // Held open by hand rather than through db.transaction(), which commits as
    // soon as its callback returns - and a committed transaction has released its
    // row locks, so the second claim would legitimately be handed the same rows
    // again. The whole property under test is about rows still locked.
    const first = await db.connect();
    try {
      await first.query('BEGIN');
      const mine = await repo.claim(first, 100);
      expect(mine.length).toBeGreaterThan(0);

      // A second connection claiming while the first still holds those locks.
      const theirs = await db.transaction(async (second) => {
        const rows = await repo.claim(second, 100);
        await second.query('ROLLBACK');
        return rows;
      });

      // SKIP LOCKED must step over the rows the other transaction holds, rather
      // than blocking on them or handing them back a second time. A plain SELECT
      // here would return exactly the rows the first claim already has.
      const myIds = new Set(mine.map((r) => r.id));
      expect(theirs.filter((r) => myIds.has(r.id))).toEqual([]);
    } finally {
      await first.query('ROLLBACK').catch(() => undefined);
      first.release();
    }
  });
});
