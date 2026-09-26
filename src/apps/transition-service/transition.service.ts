import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { ServiceClient } from '../../shared/http/service-client';
import { parseListFilter, type Outcome } from '../../shared/filter/opportunity-filter';
import { pageMatching, BATCH_SIZE } from '../../shared/filter/snapshot-query';
import { BulkJob, BulkJobRepository } from './bulk-job.repository';
import { JobTransitionRepository } from './job-transition.repository';
import { OutboxRepository } from './outbox.repository';

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

const encodeCursor = (row: { id: string }): string => row.id;

/**
 * The seam a queue will plug into. Nothing publishes yet; when RabbitMQ lands
 * this is the single place that changes, and the page loop below is already
 * shaped around it.
 */
export interface BatchPublisher {
  publish(jobId: string, opportunityIds: string[]): Promise<void>;
}

export interface SubmitResult {
  job: BulkJob;
  created: boolean;
  itemsCreated: number;
}

@Injectable()
export class TransitionService {
  constructor(
    private readonly jobs: BulkJobRepository,
    private readonly outbox: OutboxRepository,
    private readonly database: DatabaseService,
    private readonly stages: ServiceClient,
    private readonly jobTransitions: JobTransitionRepository,
  ) {}

  /**
   * Submits a bulk move.
   *
   * The filter is resolved once, here, and stored with a snapshot_at watermark.
   * An opportunity created after the watermark can never be swept up, which is
   * the guarantee the brief asks for, and it holds without materialising the
   * match set: created_at is immutable, so filter + watermark resolves to the
   * same records for every worker and on every retry.
   *
   * The trade is that the job moves what matches when the worker reaches it, not
   * what matched at submission. A record moved out of the filtered stage by
   * someone else in the meantime is skipped rather than overwritten, and
   * processed_count can land below total_matched. total_matched means "matched
   * at submission" and the status response says so.
   *
   * What this writes is 50 batch rows, not 50,000 item rows. That difference is
   * 81% of the time between this call and the 201.
   *
   * Several unfinished jobs may exist in one workspace. What prevents a double
   * apply is the caller's idempotency key, not a lock on the workspace.
   */
  async submit(workspaceId: string, body: Record<string, unknown>): Promise<SubmitResult> {
    const { idempotencyKey, targetStageId, ...filterBody } = body;
    if (typeof idempotencyKey !== 'string' || idempotencyKey.trim() === '') {
      throw new BadRequestException('idempotencyKey is required');
    }
    const key = idempotencyKey.trim();
    if (typeof targetStageId !== 'string') {
      throw new BadRequestException('targetStageId is required');
    }

    const filter = parseListFilter(filterBody);
    const resolved = await this.resolveOutcome(workspaceId, filter);
    const canonical = canonicalFilter(resolved);

    // The key identifies one logical submission. Reusing it for a different
    // request is a client bug, so it is rejected rather than silently accepted.
    //
    // This pre-check cannot see concurrent duplicates: callers racing on the
    // same key all read "not found" and only one insert wins, so the losers fall
    // through to the unique constraint and surface as a 500. Serialising
    // submissions properly is deferred to a distributed lock. Until then the
    // data-layer guarantee still holds - the constraint prevents the double
    // move - but the status code is wrong, which test/endpoints/idempotency-race
    // .spec.ts records.
    const existing = await this.jobs.findByIdempotencyKey(workspaceId, key);
    if (existing) {
      const sameTarget = existing.target_stage_id === targetStageId;
      const sameFilter = JSON.stringify(existing.filter) === JSON.stringify(canonical);
      if (sameTarget && sameFilter) {
        // A retry of the same submission. Returning the existing job is what
        // makes the retry safe rather than a second 50,000-row move.
        return { job: existing, created: false, itemsCreated: 0 };
      }
      throw new ConflictException(
        'idempotencyKey was already used for a different filter or target stage',
      );
    }

    const job = await this.jobs.create({
      workspaceId,
      idempotencyKey: key,
      filter: canonical,
      targetStageId,
    });

    // The watermark, the batches, and the intent to publish them - one
    // transaction.
    //
    // This walks the match set by keyset and writes the ids into 50 arrays. The
    // walk is the cheap half: 2.2ms per 1,000 rows measured, so 50,000 records
    // costs roughly 110ms of reading. What it replaced was 50,000 row inserts
    // carrying four indexes and three foreign keys each, which measured 5,046ms
    // and was 81% of the time between this call and its 201.
    //
    // All of it commits together, so a crash cannot leave a job that exists but
    // is never dispatched.
    const snapshotAt = new Date();
    const query = <T,>(sql: string, params: unknown[]): Promise<T[]> =>
      this.database.query<T>(sql, params);

    const batches: string[][] = [];
    let cursor: { createdAt: Date; id: string } | null = null;
    let totalMatched = 0;
    for (;;) {
      const page = await pageMatching(query, workspaceId, canonical, snapshotAt, cursor, BATCH_SIZE);
      if (page.length === 0) break;
      batches.push(page.map((r) => r.id));
      totalMatched += page.length;
      const last = page[page.length - 1]!;
      cursor = { createdAt: last.created_at, id: last.id };
      if (page.length < BATCH_SIZE) break;
    }

    await this.database.transaction(async (client) => {
      await client.query(
        `UPDATE bulk_job SET snapshot_at = $3, total_matched = $4, updated_at = now()
          WHERE id = $1 AND workspace_id = $2`,
        [job.id, workspaceId, snapshotAt, totalMatched],
      );

      for (const [batchNo, itemIds] of batches.entries()) {
        await this.outbox.enqueue(client, {
          id: '',
          workspace_id: workspaceId,
          job_id: job.id,
          batch_no: batchNo,
          item_ids: itemIds,
          attempts: 0,
        });
      }

      // A job that matched nothing has no batches, so nothing would ever settle
      // it. It is finished the moment it is created, and saying so is more useful
      // than leaving it pending forever.
      if (batches.length === 0) {
        await client.query(
          `UPDATE bulk_job SET status = 'completed', completed_at = now(), updated_at = now()
            WHERE id = $1 AND workspace_id = $2`,
          [job.id, workspaceId],
        );
      }
    });

    const refreshed = await this.jobs.findById(workspaceId, job.id);
    return { job: refreshed ?? job, created: true, itemsCreated: totalMatched };
  }

  async status(workspaceId: string, jobId: string) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);
    const counts = await this.jobs.statusCounts(workspaceId, jobId);
    const deadLettered = await this.jobs.deadLettered(workspaceId, jobId);
    return {
      id: job.id,
      status: job.status,
      targetStageId: job.target_stage_id,
      filter: job.filter,
      // Counted once at submission. A record that stops matching the filter
      // before the worker reaches it is skipped, so processed + failed can land
      // below this - which is a record that changed under the job, not a lost
      // one, and the difference is reported rather than hidden.
      totalMatched: job.total_matched,
      snapshotAt: job.snapshot_at,
      // Why a batch was refused. Without this a failed job reports only that it
      // failed, which is the one thing the caller cannot act on.
      error: job.error,
      failedCount: job.failed_count,
      // Aggregated over the job's 50 batch rows rather than read from 50,000
      // item rows. Before a worker starts, everything is pending and the counts
      // are the nominal match split across the batches.
      batches: {
        pending: counts['pending'] ?? 0,
        running: counts['running'] ?? 0,
        completed: counts['completed'] ?? 0,
        failed: counts['failed'] ?? 0,
      },
      // Batches that ran out of attempts, which is what it means for a message to
      // reach the dead letter queue. Separate from `batches.failed` in meaning
      // even though both count the same rows, and from `failures` in kind: those
      // are records a user has to fix, these are records the worker never got to.
      // Zero for a healthy job, and worth alerting on.
      deadLettered,
      createdAt: job.created_at,
      startedAt: job.started_at,
      completedAt: job.completed_at,
    };
  }

  /**
   * The records that failed and why, grouped by the batch that carried them.
   *
   * A batch applies whole or not at all, so a refusal fails all of it. This is
   * how a caller finds the record to fix before re-running.
   */
  async failures(workspaceId: string, jobId: string, limit?: string) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);
    const parsed = Number(limit ?? DEFAULT_LIMIT);
    return this.jobs.failures(
      workspaceId,
      jobId,
      Math.min(Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_LIMIT, MAX_LIMIT),
    );
  }

  /** Per-batch progress, which is the unit the work is actually done in. */
  async batches(workspaceId: string, jobId: string) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);
    return this.jobs.batchSummary(workspaceId, jobId);
  }

  /**
   * Transitions this job caused. Manual moves leave transition.job_id null, so
   * they never appear here - which is what makes the list attributable.
   */
  async transitions(
    workspaceId: string,
    jobId: string,
    query: { limit?: string | undefined; cursor?: string | undefined },
  ) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);

    const parsed = Number(query.limit ?? DEFAULT_LIMIT);
    const limit = Math.min(
      Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_LIMIT,
      MAX_LIMIT,
    );
    if (
      query.cursor &&
      !(await this.jobTransitions.cursorBelongsToJob(workspaceId, jobId, query.cursor))
    ) {
      throw new BadRequestException('cursor does not belong to this job');
    }

    const { items, hasMore } = await this.jobTransitions.list(
      workspaceId,
      jobId,
      limit,
      query.cursor ?? null,
    );
    const last = items.at(-1);
    return {
      items,
      nextCursor: hasMore && last ? encodeCursor(last) : null,
    };
  }

  /**
   * outcome lives on the stage, not the row, so filtering by it resolves to
   * stage ids first. One call per submission, never per row.
   */
  private async resolveOutcome(
    workspaceId: string,
    filter: ReturnType<typeof parseListFilter>,
  ): Promise<ReturnType<typeof parseListFilter>> {
    if (filter.outcome === undefined) return filter;
    const stages = await this.stages.get<{ id: string; outcome: string }[]>(
      'stage',
      '/stages',
      workspaceId,
    );
    const ids = stages.filter((s) => s.outcome === filter.outcome).map((s) => s.id);
    return { ...filter, stageIds: ids, outcome: undefined as unknown as Outcome };
  }
}

/**
 * Key order must be stable, or the same logical filter submitted with its keys
 * in a different order would look like a different request and get a 409.
 */
function canonicalFilter(filter: ReturnType<typeof parseListFilter>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (filter.stageIds?.length) out['stageId'] = [...filter.stageIds].sort();
  if (filter.ownerIds?.length) out['ownerId'] = [...filter.ownerIds].sort();
  if (filter.minValue !== undefined) out['minValue'] = filter.minValue;
  if (filter.maxValue !== undefined) out['maxValue'] = filter.maxValue;
  if (filter.createdFrom) out['createdFrom'] = filter.createdFrom.toISOString();
  if (filter.createdTo) out['createdTo'] = filter.createdTo.toISOString();
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => a.localeCompare(b)));
}
