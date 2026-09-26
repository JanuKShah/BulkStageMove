import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import type { PoolClient } from 'pg';

export interface OutboxRow {
  id: string;
  workspace_id: string;
  job_id: string;
  batch_no: number;
  item_count: number;
  attempts: number;
}

/**
 * The dispatch outbox.
 *
 * A page of items and the intent to publish it are written in one transaction,
 * so a crash cannot leave a page that exists but was never announced. The relay
 * publishes and stamps published_at, which makes delivery at-least-once; the
 * worker claiming pending items is what makes processing effectively-once. The
 * alternative - publishing straight after the insert - has a window where a
 * crash strands a page of pending items and the job never leaves pending.
 */
@Injectable()
export class OutboxRepository {
  constructor(private readonly db: DatabaseService) {}

  /** Must share the caller's transaction with the item insert. */
  async enqueue(client: PoolClient, row: OutboxRow): Promise<void> {
    await client.query(
      `INSERT INTO bulk_job_outbox
         (workspace_id, job_id, batch_no, item_count, attempts)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (job_id, batch_no) DO NOTHING`,
      [row.workspace_id, row.job_id, row.batch_no, row.item_count, row.attempts],
    );
  }

  /** Unsent rows, oldest first, so batches are published in submission order. */
  async unpublished(limit: number): Promise<OutboxRow[]> {
    return this.db.query<OutboxRow>(
      `SELECT id, workspace_id, job_id, batch_no, item_count, attempts
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
