import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { ServiceClient } from '../../shared/http/service-client';
import { parseListFilter, type Outcome } from '../../shared/filter/opportunity-filter';
import { BulkJob, BulkJobRepository } from './bulk-job.repository';
import { BulkJobItemRepository } from './bulk-job-item.repository';
import { JobTransitionRepository } from './job-transition.repository';

const PAGE_SIZE = 1000;
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
    private readonly items: BulkJobItemRepository,
    private readonly database: DatabaseService,
    private readonly stages: ServiceClient,
    private readonly jobTransitions: JobTransitionRepository,
  ) {}

  /**
   * Submits a bulk move.
   *
   * The filter is resolved once, here, and frozen into bulk_job_item. That is
   * the snapshot decision: the job moves exactly the set that existed at
   * submission, so "what did this job change" stays answerable, and an
   * opportunity created afterwards is not swept up.
   *
   * The job row is written before any items, so a crash mid-sweep leaves a
   * partial job rather than items with nothing pointing at them. The status
   * endpoint reports the real per-status counts, which is what makes a partial
   * job visible instead of silently short.
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

    let cursor: { createdAt: Date; id: string } | null = null;
    let itemsCreated = 0;
    for (;;) {
      const page = await this.items.page(workspaceId, resolved, cursor, PAGE_SIZE);
      if (page.length === 0) break;

      const ids = page.map((p) => p.id);
      const inserted = await this.database.transaction((client) =>
        this.items.insertPage(client, workspaceId, job.id, ids),
      );
      itemsCreated += inserted;
      await this.jobs.addToTotal(workspaceId, job.id, inserted);

      const last = page[page.length - 1]!;
      cursor = { createdAt: last.created_at, id: last.id };
      if (page.length < PAGE_SIZE) break;
    }

    const refreshed = await this.jobs.findById(workspaceId, job.id);
    return { job: refreshed ?? job, created: true, itemsCreated };
  }

  async status(workspaceId: string, jobId: string) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);
    const counts = await this.jobs.statusCounts(workspaceId, jobId);
    return {
      id: job.id,
      status: job.status,
      targetStageId: job.target_stage_id,
      filter: job.filter,
      totalMatched: job.total_matched,
      items: {
        pending: counts['pending'] ?? 0,
        running: counts['running'] ?? 0,
        completed: counts['completed'] ?? 0,
        failed: counts['failed'] ?? 0,
      },
      createdAt: job.created_at,
      startedAt: job.started_at,
      completedAt: job.completed_at,
    };
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
