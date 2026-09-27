import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import type { PoolClient } from 'pg';

export interface OutboxRow {
  id: string;
  workspace_id: string;
  job_id: string;
  batch_no: number;
  item_ids: string[];
  attempts: number;
}

/**
 * A batch the relay has claimed, and may therefore publish.
 *
 * Deliberately not `OutboxRow`: this carries `item_count` and not `item_ids`.
 * A row of item ids is a thousand uuids, and the relay only ever wanted to know
 * how many there were - so selecting the array shipped about 37KB per batch over
 * the wire to run `.length` on it in JavaScript. `cardinality` answers it in the
 * database and the ids never leave.
 */
export interface ClaimedBatch {
  id: string;
  workspace_id: string;
  job_id: string;
  batch_no: number;
  item_count: number;
  attempts: number;
}

/**
 * The dispatch outbox, and the record of what each batch covers.
 *
 * A batch's records and the intent to publish it are written in one transaction,
 * so a crash cannot leave a batch that exists but was never announced. The relay
 * publishes and stamps published_at, which makes delivery at-least-once; the
 * worker's claim is what makes processing effectively-once. The alternative -
 * publishing straight after the insert - has a window where a crash strands a
 * batch and the job never leaves pending.
 */
@Injectable()
export class OutboxRepository {
  constructor(private readonly db: DatabaseService) {}

  /** Must share the caller's transaction with the item insert. */
  async enqueue(client: PoolClient, row: OutboxRow): Promise<void> {
    await client.query(
      `INSERT INTO bulk_job_outbox
         (workspace_id, job_id, batch_no, item_ids, attempts)
       VALUES ($1, $2, $3, $4::uuid[], $5)
       ON CONFLICT (job_id, batch_no) DO NOTHING`,
      [row.workspace_id, row.job_id, row.batch_no, row.item_ids, row.attempts],
    );
  }

  /**
   * Claims up to `limit` batches to publish, locking them until the caller
   * commits.
   *
   * FOR UPDATE SKIP LOCKED is what makes more than one relay replica safe. Two
   * replicas running this at the same time get *disjoint* rows rather than the
   * same rows, so neither publishes a batch the other is already publishing.
   * Without it every replica read the same unpublished rows and every batch went
   * out once per replica - correct only because the worker's own claim made the
   * duplicates harmless, and wasteful in proportion to the replica count.
   *
   * The lock is held to the end of the caller's transaction, so the rows must be
   * marked published or failed before that commit. A replica that dies mid-pass
   * rolls back, the rows unlock, and the next pass republishes them - at-least-once
   * again, which is the guarantee the outbox already made.
   *
   * ORDER BY attempts, then created_at. A batch whose publish failed keeps
   * published_at NULL, so ordering by created_at alone left it at the front of the
   * queue permanently, and it was re-selected on every tick and retried ahead of
   * work that had never been tried. Ordering by attempts puts a fresh batch ahead
   * of a retried one, so a single poisonous batch stops blocking its own job.
   */
  async claim(client: PoolClient, limit: number): Promise<ClaimedBatch[]> {
    const { rows } = await client.query<ClaimedBatch>(
      `SELECT id, workspace_id, job_id, batch_no,
              cardinality(item_ids)::int AS item_count, attempts
         FROM bulk_job_outbox
        WHERE published_at IS NULL
        ORDER BY attempts ASC, created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [limit],
    );
    return rows;
  }

  /**
   * Gives up on batches that have exhausted the relay's attempt budget.
   *
   * Without this, capping attempts would be worse than not capping them: a row at
   * the cap with published_at still NULL is never published and never removed, so
   * its records are never moved, its job never leaves pending, and nothing
   * anywhere records why. Marking it failed routes it into the surfaces that
   * already exist - the job's deadLettered count, the failures endpoint, and
   * POST /bulk-moves/:id/retry-failed.
   */
  async abandonExhausted(client: PoolClient, maxAttempts: number): Promise<number> {
    const { rows } = await client.query<{ job_id: string; workspace_id: string; n: number }>(
      `UPDATE bulk_job_outbox
          SET status = 'failed', error = $2::text,
              failed_count = cardinality(item_ids), completed_at = now(), updated_at = now()
        WHERE published_at IS NULL AND attempts >= $1
        RETURNING job_id, workspace_id, cardinality(item_ids)::int AS n`,
      [maxAttempts, `relay gave up after ${maxAttempts} attempts`],
    );
    if (rows.length === 0) return 0;

    const jobIds = [...new Set(rows.map((r) => r.job_id))];
    const workspaces = [...new Set(rows.map((r) => r.workspace_id))];
    const byJob = new Map<string, number>();
    for (const r of rows) byJob.set(r.job_id, (byJob.get(r.job_id) ?? 0) + r.n);

    // The records are not moved, so they belong in failed_count. The worker does
    // the same when it gives up on a batch, and a job whose every record failed
    // has to read as failed rather than completed.
    for (const [jobId, n] of byJob) {
      await client.query(
        `UPDATE bulk_job
            SET failed_count = failed_count + $2, error = $3::text, updated_at = now()
          WHERE id = $1`,
        [jobId, n, `relay gave up after ${maxAttempts} attempts`],
      );
    }

    // Settle each job the way the worker's settleJob does, because a job whose
    // batches are all abandoned would otherwise sit in pending for ever with
    // nothing left that any worker could take.
    await client.query(
      `UPDATE bulk_job j
          SET status = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_outbox b
                              WHERE b.job_id = j.id AND b.status IN ('pending','running'))
                  THEN 'running'
                WHEN j.processed_count = 0 AND j.failed_count > 0 THEN 'failed'
                ELSE 'completed' END,
              completed_at = CASE
                WHEN EXISTS (SELECT 1 FROM bulk_job_outbox b
                              WHERE b.job_id = j.id AND b.status IN ('pending','running'))
                  THEN NULL
                ELSE now() END,
              updated_at = now()
        WHERE j.id = ANY($1::uuid[]) AND j.workspace_id = ANY($2::uuid[])
          AND j.status NOT IN ('completed','failed','preparing')`,
      [jobIds, workspaces],
    );
    return rows.length;
  }

  /** Takes the caller's transaction, so the claim stays locked until commit. */
  async markPublished(client: PoolClient, id: string): Promise<void> {
    await client.query('UPDATE bulk_job_outbox SET published_at = now() WHERE id = $1', [id]);
  }

  /**
   * Records a failed publish attempt. The row stays unpublished on purpose - it
   * is the record that the batch still has to go out - and the next claim orders
   * it behind anything that has not been tried yet.
   */
  async markFailed(client: PoolClient, id: string): Promise<void> {
    await client.query('UPDATE bulk_job_outbox SET attempts = attempts + 1 WHERE id = $1', [id]);
  }
}
