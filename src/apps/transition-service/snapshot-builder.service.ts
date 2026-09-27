import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { RABBIT_CONFIG, type RabbitConfig } from '../../shared/rabbit/rabbit.config';
import { pageMatching, parseStoredFilter, BATCH_SIZE } from '../../shared/filter/snapshot-query';

/**
 * Builds a job's batches after the response has been sent.
 *
 * Submission writes the job row and returns. The walk that turns a filter into
 * batches is the expensive half - 0.56s to 2.30s at 50,000 records - and none of
 * it has to happen before the caller gets an id.
 *
 * It is in-process rather than on the broker because the cursor on the job row
 * makes an interrupted build recoverable: the next sweep picks up where this one
 * stopped. A broker message would be more durable and would add a second thing
 * that can fail.
 *
 * Each batch is committed with the cursor that follows it, in one transaction.
 * That is what makes the cursor trustworthy - it can never claim progress the data
 * does not have. It costs one commit per batch, which no longer matters because
 * none of this is on anyone's critical path any more.
 */
@Injectable()
export class SnapshotBuilder implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SnapshotBuilder.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(
    private readonly db: DatabaseService,
    @Inject(RABBIT_CONFIG) private readonly config: RabbitConfig,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.sweep();
    }, this.config.snapshotSweepIntervalMs);
    // Do not hold the process open purely for the sweep.
    this.timer.unref();
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
  }

  /**
   * Finishes every job still preparing.
   *
   * A sweep that is already running is left alone rather than queued: a second
   * concurrent sweep would walk the same cursor and write the same batch numbers,
   * and the advisory lock would turn that into wasted work rather than a
   * correctness problem - but there is no reason to do it.
   */
  async sweep(): Promise<void> {
    if (this.running || this.stopped) return;
    this.running = true;
    try {
      const jobs = await this.db.query<{ id: string; workspace_id: string }>(
        `SELECT id, workspace_id FROM bulk_job
          WHERE status = 'preparing'
          ORDER BY created_at
          LIMIT 5`,
      );
      for (const job of jobs) {
        if (this.stopped) return;
        try {
          await this.build(job.id, job.workspace_id);
        } catch (error) {
          // Left preparing on purpose. The next sweep retries from the cursor, and
          // a job that cannot be built is one an operator should see rather than
          // one that silently completed having moved nothing.
          this.logger.error(
            `snapshot build failed for job ${job.id}: ${(error as Error).message}`,
          );
        }
      }
    } finally {
      this.running = false;
    }
  }

  /**
   * Walks the job's filter into batches, resuming from its cursor if it has one.
   *
   * Reads a page, writes it as a batch and advances the cursor, then repeats. The
   * loop ends on a short page rather than an empty one, so a match count that is an
   * exact multiple of the page size does not cost a final empty round trip.
   */
  async build(jobId: string, workspaceId: string): Promise<void> {
    const spec = await this.db.query<{
      filter: unknown;
      snapshot_at: Date;
      snapshot_cursor: Date | null;
      snapshot_cursor_id: string | null;
    }>(
      `SELECT filter, snapshot_at, snapshot_cursor, snapshot_cursor_id
         FROM bulk_job WHERE id = $1 AND workspace_id = $2`,
      [jobId, workspaceId],
    );
    const job = spec[0];
    if (!job) return;

    const filter = parseStoredFilter(job.filter);
    const query = <T,>(sql: string, params: unknown[]): Promise<T[]> =>
      this.db.query<T>(sql, params);

    let cursor: { createdAt: Date; id: string } | null =
      job.snapshot_cursor && job.snapshot_cursor_id
        ? { createdAt: job.snapshot_cursor, id: job.snapshot_cursor_id }
        : null;
    let batchNo = 0;
    let matched = 0;

    const resumeFrom = cursor;
    if (resumeFrom) {
      const prior = await this.db.query<{ n: number }>(
        `SELECT coalesce(max(batch_no), -1)::int AS n FROM bulk_job_outbox
          WHERE job_id = $1 AND workspace_id = $2`,
        [jobId, workspaceId],
      );
      batchNo = (prior[0]?.n ?? -1) + 1;
    }

    for (;;) {
      const page = await pageMatching(
        query,
        workspaceId,
        filter,
        job.snapshot_at,
        cursor,
        BATCH_SIZE,
      );
      if (page.length === 0) break;

      const last = page[page.length - 1]!;
      const nextCursor = { createdAt: last.created_at, id: last.id };

      await this.db.transaction(async (client) => {
        await client.query(
          `INSERT INTO bulk_job_outbox (workspace_id, job_id, batch_no, item_ids)
           VALUES ($1, $2, $3, $4::uuid[])
           ON CONFLICT (job_id, batch_no) DO NOTHING`,
          [workspaceId, jobId, batchNo, page.map((r) => r.id)],
        );
        // Same transaction as the batch. A cursor ahead of the data would skip
        // records on resume; a cursor behind it would duplicate them.
        await client.query(
          `UPDATE bulk_job
              SET snapshot_cursor = $3, snapshot_cursor_id = $4, updated_at = now()
            WHERE id = $1 AND workspace_id = $2`,
          [jobId, workspaceId, nextCursor.createdAt, nextCursor.id],
        );
      });

      matched += page.length;
      cursor = nextCursor;
      batchNo += 1;
      if (page.length < BATCH_SIZE) break;
    }

    // total_matched is what actually landed in batches, not a count taken before
    // the walk. A record can leave the filter while the walk is running, so a
    // pre-count would be a number that was already stale by the time it was sent.
    await this.db.transaction(async (client) => {
      await client.query(
        `UPDATE bulk_job j
            SET total_matched = COALESCE(agg.records, 0),
                snapshot_cursor = NULL,
                snapshot_cursor_id = NULL,
                status = CASE
                  WHEN COALESCE(agg.batches, 0) = 0 THEN 'completed'
                  ELSE 'pending'
                END,
                completed_at = CASE
                  WHEN COALESCE(agg.batches, 0) = 0 THEN now()
                  ELSE NULL
                END,
                updated_at = now()
           FROM (SELECT count(*)::int AS batches, coalesce(sum(cardinality(item_ids)), 0)::int AS records
                   FROM bulk_job_outbox WHERE job_id = $1 AND workspace_id = $2) agg
          WHERE j.id = $1 AND j.workspace_id = $2`,
        [jobId, workspaceId],
      );
    });

    this.logger.log(
      `job ${jobId} snapshot built: ${matched} records in ${batchNo} batches`,
    );
  }
}
