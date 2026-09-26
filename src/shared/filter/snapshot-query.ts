/**
 * The bulk job's query, in one place.
 *
 * A job stores its filter as jsonb and its watermark as snapshot_at. Both the
 * submission count and the worker's per-batch paging have to turn that pair back
 * into SQL, and they have to agree exactly - if the count and the page resolved
 * different sets, a job would report a total it never reached for a reason that
 * looks like a bug.
 *
 * So the predicate is built once, here, and both callers use it.
 *
 * A batch is a position, not a stored set of ids: batch N is the Nth page of
 * this query. That is sound because created_at is immutable, so a filter plus a
 * watermark resolves to the same records for every worker and on every retry,
 * with nothing written per record.
 */

/**
 * Records per batch. One definition, shared by both sides.
 *
 * Submit uses it to decide how many batches a job has; the worker uses it to
 * derive which records batch N covers. If those two ever disagree the worker
 * reads pages that do not line up with the batches that were dispatched, and
 * records get processed twice or not at all - so it lives here rather than being
 * declared independently in each service.
 */
export const BATCH_SIZE = 1000;

export interface StoredFilter {
  stageId?: string[];
  ownerId?: string[];
  minValue?: number;
  maxValue?: number;
  createdFrom?: string;
  createdTo?: string;
}

export interface OpportunityRef {
  id: string;
  stage_id: string;
}

/**
 * Builds the WHERE clause for a job's filter at its watermark.
 *
 * `params` starts as the caller's fixed arguments, so the numbering continues
 * from wherever the caller left it rather than restarting and colliding.
 */
function buildPredicate(
  params: unknown[],
  workspaceParam: number,
  filter: StoredFilter,
  snapshotParam: number,
): string {
  let where = `workspace_id = $${workspaceParam} AND created_at <= $${snapshotParam}`;

  const add = (clause: string, value: unknown): void => {
    params.push(value);
    where += ` AND ${clause.replace('$?', `$${params.length}`)}`;
  };

  if (filter.stageId?.length) add('stage_id = ANY($?::uuid[])', filter.stageId);
  if (filter.ownerId?.length) add('owner_id = ANY($?::uuid[])', filter.ownerId);
  if (filter.minValue !== undefined) add('value >= $?', filter.minValue);
  if (filter.maxValue !== undefined) add('value <= $?', filter.maxValue);
  if (filter.createdFrom) add('created_at >= $?', filter.createdFrom);
  if (filter.createdTo) add('created_at <= $?', filter.createdTo);

  return where;
}

/**
 * How many records this job matched at submission.
 *
 * One indexed count, replacing fifty thousand item inserts and fifty
 * counter updates. It is a prediction rather than a promise: a record moved out
 * of the filtered stage afterwards is skipped by the worker, which is why
 * total_matched is documented as "matched at submission" and why
 * processed_count is allowed to land below it.
 */
export async function countMatching(
  query: <T>(sql: string, params: unknown[]) => Promise<T[]>,
  workspaceId: string,
  filter: StoredFilter,
  snapshotAt: Date,
): Promise<number> {
  const params: unknown[] = [workspaceId, snapshotAt];
  const where = buildPredicate(params, 1, filter, 2);
  const rows = await query<{ n: number }>(
    `SELECT count(*)::int AS n FROM opportunity WHERE ${where}`,
    params,
  );
  return rows[0]?.n ?? 0;
}

/**
 * One batch's worth of records, read forwards by keyset.
 *
 * Used only at submission, to fill the batch arrays. Keyset rather than OFFSET
 * because the sweep walks the whole match set: at offset 49,000 an OFFSET query
 * reads and discards 49,000 index entries to return the last thousand, which
 * makes the sweep quadratic in the number of batches. Measured at 2.2ms for a
 * 1,000-row keyset page against a 50,000-row table.
 *
 * The worker does not use this - it reads the ids off its batch row - so there is
 * no OFFSET anywhere in the job path.
 */
export async function pageMatching(
  query: <T>(sql: string, params: unknown[]) => Promise<T[]>,
  workspaceId: string,
  filter: StoredFilter,
  snapshotAt: Date,
  after: { createdAt: Date; id: string } | null,
  limit: number,
): Promise<(OpportunityRef & { created_at: Date })[]> {
  const params: unknown[] = [workspaceId, snapshotAt];
  let where = buildPredicate(params, 1, filter, 2);

  if (after) {
    // The cursor position is resolved by subquery, never by binding a JS Date.
    // created_at is timestamptz and carries microseconds while a Date carries
    // milliseconds, so binding it truncates: every remaining row compares greater
    // than the truncated cursor, the same page returns for ever, and every
    // subsequent page conflicts.
    params.push(after.id);
    where += ` AND (created_at, id) > (
      SELECT created_at, id FROM opportunity WHERE id = $${params.length} AND workspace_id = $1)`;
  }
  params.push(limit);

  return query<OpportunityRef & { created_at: Date }>(
    `SELECT id, stage_id, created_at FROM opportunity WHERE ${where}
     ORDER BY created_at, id LIMIT $${params.length}`,
    params,
  );
}
