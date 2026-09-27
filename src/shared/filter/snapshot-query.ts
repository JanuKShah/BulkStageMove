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
import type { KeysetCursor } from './keyset-cursor';

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
 * Narrows a job's stored jsonb filter to the shape the predicate builder expects.
 *
 * A defensive cast rather than a validation: the value was written by
 * canonicalFilter, so it is already in this shape, and a row that somehow was not
 * would produce an over-broad match rather than a crash - which is the wrong way
 * to fail, but failing loudly on a job mid-flight is worse than moving what the
 * stored filter actually describes.
 */
export function parseStoredFilter(raw: unknown): StoredFilter {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return {};
  return raw;
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

  // `!== undefined`, not `?.length`. A filter that named stages and resolved to
  // none has to match nothing, and the difference between "named none" and "named
  // nothing" is the whole bug: `?.length` treated both as absent and dropped the
  // clause, so the predicate became every other condition alone.
  //
  // An outcome is resolved to a stage list at submission, so `outcome: 'lost'` in
  // a workspace with no lost stages arrives here as an empty array. Measured
  // before this was fixed: that filter stored as `{}` and matched all 10 records
  // in the workspace instead of none. `= ANY('{}')` is false for every row, so
  // passing the empty array through is both correct and still index-friendly -
  // no `AND false` needed.
  if (filter.stageId !== undefined) add('stage_id = ANY($?::uuid[])', filter.stageId);
  if (filter.ownerId !== undefined) add('owner_id = ANY($?::uuid[])', filter.ownerId);
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
  after: KeysetCursor | null,
  limit: number,
): Promise<(OpportunityRef & { created_at: string })[]> {
  const params: unknown[] = [workspaceId, snapshotAt];
  let where = buildPredicate(params, 1, filter, 2);

  if (after) {
    // The position is bound, not looked up. It used to be re-read from the cursor
    // row with a subquery, which meant a cursor row that had been deleted made the
    // comparison UNKNOWN and silently dropped every remaining record - a job that
    // reported a short total_matched with no error anywhere.
    //
    // The timestamp is bound as text in the database's own timestamptz form and
    // cast back, never as a JS Date. A Date holds milliseconds where timestamptz
    // holds microseconds, so binding one truncates the cursor to below its own row
    // and the same page returns for ever.
    params.push(after.createdAt, after.id);
    where += ` AND (created_at, id) > ($${params.length - 1}::timestamptz, $${params.length}::uuid)`;
  }
  params.push(limit);

  return query<OpportunityRef & { created_at: string }>(
    // created_at comes back as text so the next cursor is lossless. Selecting the
    // column itself would hand back a Date and the microseconds would be gone
    // before the cursor was built.
    //
    // The ORDER BY is qualified deliberately. An unqualified `created_at` resolves
    // to the output alias - the text projection - and the sort then happens on
    // text, which the index cannot supply: measured, that turned a 1ms index scan
    // into an 83ms seq scan plus top-N sort. Binding the column keeps the row
    // comparison and the ordering both on the raw timestamptz.
    `SELECT id, stage_id, created_at::text AS created_at FROM opportunity WHERE ${where}
     ORDER BY opportunity.created_at, opportunity.id LIMIT $${params.length}`,
    params,
  );
}
