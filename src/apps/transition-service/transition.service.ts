import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';
import { isUniqueViolation } from '../../shared/database/unique-violation';
import { ServiceClient } from '../../shared/http/service-client';
import { currentRequestId } from '../../shared/http/request-context';
import { parseListFilter, type Outcome } from '../../shared/filter/opportunity-filter';
import { decodeCursor } from '../../shared/filter/keyset-cursor';
import { BulkJob, BulkJobRepository } from './bulk-job.repository';
import { JobTransitionRepository } from './job-transition.repository';
import { OutboxRepository } from './outbox.repository';
import { OutboxRelay } from './outbox-relay';
import { SnapshotBuilder } from './snapshot-builder.service';

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
    private readonly builder: SnapshotBuilder,
    private readonly relay: OutboxRelay,
  ) {}

  /**
   * Submits a bulk move.
   *
   * Writes the job row and returns. The walk that turns the filter into batches
   * happens afterwards, in SnapshotBuilder, and is resumable from a cursor on the
   * job row.
   *
   * The response is therefore immediate and says so: status is 'preparing', and
   * there is no count, because the count is not known yet and a pre-count would
   * already be stale by the time it was sent - a record can leave the filter
   * while the walk runs. total_matched becomes the number that actually landed in
   * batches, and the caller reads it from the status endpoint.
   *
   * This also removes a failure mode rather than adding one. The walk used to run
   * here, outside the transaction that wrote the batches, so a process that died
   * during it left a job with no batches, nothing for the relay to publish, and
   * no way to notice it had stalled. It is now someone else's job, retried.
   *
   * The watermark is still set at submission, so an opportunity created after this
   * point can never be swept up - which is the guarantee the brief asks for.
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
    const existing = await this.jobs.findByIdempotencyKey(workspaceId, key);
    if (existing) {
      return this.replayOrConflict(existing, targetStageId, canonical);
    }

    let job: BulkJob;
    try {
      job = await this.jobs.create({
        workspaceId,
        idempotencyKey: key,
        filter: canonical,
        targetStageId,
        // Read from the request's async context and stored on the row, because the
        // build that runs after this returns is outside that context entirely. This
        // is the join between "someone POSTed this" and "the batches were written".
        correlationId: currentRequestId(),
      });
    } catch (error) {
      // A caller that raced a winner on the same key got past the pre-check above,
      // because both read "not found" before either inserted. The constraint is
      // what actually prevents the double move; this is only about reporting it as
      // a replay rather than as a server fault.
      //
      // Scoped to this constraint by name, so any other unique violation - the
      // primary key, say - still propagates as the 500 it is.
      if (!isUniqueViolation(error, 'bulk_job_workspace_idempotency_uniq')) throw error;

      // Postgres blocks the conflicting insert until the other transaction
      // resolves, so a violation means that transaction committed and its row is
      // visible to us now. The null check is unreachable, and rethrowing beats
      // inventing an answer: if it ever were null, the honest report is the
      // violation we were actually given.
      const winner = await this.jobs.findByIdempotencyKey(workspaceId, key);
      if (!winner) throw error;
      return this.replayOrConflict(winner, targetStageId, canonical);
    }

    // Kick the build without waiting for it. The sweep would pick the job up
    // anyway, and doing it here as well only removes the wait for the common case
    // where nothing else is preparing.
    void this.builder.sweep();

    return { job, created: true, itemsCreated: 0 };
  }

  /**
   * Decides what a key that is already on a job means.
   *
   * The same request is a retry and gets the job back; a different one is a
   * client bug and is refused. Shared by the pre-check and the race path so both
   * answer identically - a replay must not depend on whether the caller happened
   * to race, or the same request would get two different answers.
   */
  private replayOrConflict(
    existing: BulkJob,
    targetStageId: string,
    canonical: Record<string, unknown>,
  ): SubmitResult {
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

  /**
   * Re-queues the batches that ran out of attempts, for a caller to ask for
   * explicitly rather than for the system to do behind their back.
   *
   * The reset alone is enough to get the work going: it clears `published_at`, so
   * the relay's next pass publishes those rows like any other. Returns how many
   * batches and records went back, which is zero when there was nothing to do.
   */
  async retryFailed(workspaceId: string, jobId: string) {
    const job = await this.jobs.findById(workspaceId, jobId);
    if (!job) throw new NotFoundException(`bulk job ${jobId} not found`);
    const retried = await this.jobs.retryFailed(workspaceId, jobId);
    // Nudged rather than waited on, so the common case - one failed batch, one
    // idle relay - does not pay a full tick. The sweep timer is the fallback, and
    // the reset is durable either way.
    void this.relay.tick();
    return { jobId, retriedBatches: retried.batches, retriedRecords: retried.records };
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
      // Zero while preparing, and only then the number of records that actually
      // landed in batches. Not counted up front: a record can leave the filter
      // while the walk runs, so a pre-count would be stale by the time it was
      // sent. Read it here rather than on the submit response for the same reason.
      totalMatched: job.total_matched,
      snapshotAt: job.snapshot_at,
      // Non-null only while preparing, and the sign that a job is building rather
      // than stalled. A client polling this should keep waiting.
      snapshotInProgress: job.status === 'preparing',
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
      // Batches that ran out of attempts. Separate from `batches.failed` in
      // meaning even though both count the same rows, and from `failures` in
      // kind: those are records a user has to fix, these are records the worker
      // never got to. Zero for a healthy job, and worth alerting on.
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
    // Decoded once here so the "does this cursor belong to this job" check has an
    // id to look up, and so a malformed cursor is rejected as a bad request rather
    // than being passed down to a query that would treat it as no cursor at all.
    const at = query.cursor ? decodeCursor(query.cursor) : null;
    if (query.cursor && !at) {
      throw new BadRequestException('cursor is not a valid pagination cursor');
    }
    if (at && !(await this.jobTransitions.cursorBelongsToJob(workspaceId, jobId, at.id))) {
      throw new BadRequestException('cursor does not belong to this job');
    }

    const { items, nextCursor } = await this.jobTransitions.list(
      workspaceId,
      jobId,
      limit,
      query.cursor ?? null,
    );
    return { items, nextCursor };
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
  // `!== undefined`, not `?.length`, and it is load-bearing. An outcome is resolved
  // to a stage list before this runs, so `outcome: 'lost'` in a workspace with no
  // lost stage arrives with stageIds set to an empty array. Dropping it here
  // stored the filter as `{}`, and a job with no stage filter matches the whole
  // workspace - so asking to move the lost deals moved everything instead.
  //
  // Keeping the empty list is also what makes the filter hashable as distinct: a
  // job for "lost" and a job for the whole workspace must not share an identity,
  // and with the key omitted they were the same filter.
  if (filter.stageIds !== undefined) out['stageId'] = [...filter.stageIds].sort();
  if (filter.ownerIds !== undefined) out['ownerId'] = [...filter.ownerIds].sort();
  if (filter.minValue !== undefined) out['minValue'] = filter.minValue;
  if (filter.maxValue !== undefined) out['maxValue'] = filter.maxValue;
  if (filter.createdFrom) out['createdFrom'] = filter.createdFrom.toISOString();
  if (filter.createdTo) out['createdTo'] = filter.createdTo.toISOString();
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => a.localeCompare(b)));
}
