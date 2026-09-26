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

  /** Unsent rows, oldest first, so batches are published in submission order. */
  async unpublished(limit: number): Promise<OutboxRow[]> {
    return this.db.query<OutboxRow>(
      `SELECT id, workspace_id, job_id, batch_no, item_ids, attempts
         FROM bulk_job_outbox
        WHERE published_at IS NULL
        ORDER BY created_at
        LIMIT $1`,
      [limit],
    );
  }

  async markPublished(id: string): Promise<void> {
    await this.db.query('UPDATE bulk_job_outbox SET published_at = now() WHERE id = $1', [id]);
  }

  async markFailed(id: string): Promise<void> {
    await this.db.query('UPDATE bulk_job_outbox SET attempts = attempts + 1 WHERE id = $1', [id]);
  }
}
