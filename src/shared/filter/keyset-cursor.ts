/**
 * Keyset cursors that carry their own position.
 *
 * A keyset cursor is the position of the last row a page returned, and the
 * position is the pair (created_at, id). This exists because that pair used to be
 * re-derived from the row instead of carried:
 *
 *   WHERE (created_at, id) > (SELECT created_at, id FROM t WHERE id = $cursor)
 *
 * which fails two ways.
 *
 * If the cursor row is deleted or purged before the next page - and rows are
 * deleted here, `ON DELETE CASCADE` takes them with their workspace - the
 * subquery yields NULL, `(created_at, id) > NULL` is UNKNOWN, and the predicate
 * matches nothing. Not "the page is wrong": *every remaining row is silently
 * dropped*, so a caller paging a large set quietly gets a short result and no
 * error.
 *
 * And the re-read costs a lookup per page to learn something the caller already
 * had.
 *
 * So the position travels with the cursor instead. That only works if the
 * timestamp survives the trip, and this is the whole difficulty: `timestamptz`
 * carries microseconds and a JavaScript `Date` carries milliseconds. Bind a
 * `Date` and the cursor is truncated to below its own row, that row compares
 * greater than the truncated cursor, and the same page returns for ever.
 *
 * So the timestamp is carried as text in the database's own representation and
 * bound back as a `timestamptz`, which round-trips exactly. It is never a `Date`
 * on this path.
 */

/** A position in the (created_at, id) order. `createdAt` is timestamptz text. */
export interface KeysetCursor {
  createdAt: string;
  id: string;
}

/**
 * Projects a timestamptz column to its own text form.
 *
 * Alias it to the same name the code already uses, so a row's `created_at` is
 * the lossless form and nothing downstream has to know the difference.
 */
export const CURSOR_AT = 'created_at::text AS created_at';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * A timestamp is anything Postgres will cast back. Deliberately loose on the
 * format and strict on the id: the value is bound as a `timestamptz`, so Postgres
 * is the authority on what it accepts, and re-implementing its parser here would
 * only be a way to disagree with it.
 */
const TIMESTAMPTZ = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?([+-]\d{2}(:\d{2})?|Z)$/;

/**
 * Encodes a position as an opaque token for a client to echo back.
 *
 * Opaque on purpose. A caller should never parse this or depend on its shape;
 * it exists so the position can travel without a second round trip, not so the
 * timestamp can be inspected client-side.
 */
export function encodeCursor(cursor: KeysetCursor): string {
  return Buffer.from(`${cursor.createdAt}|${cursor.id}`, 'utf8').toString('base64url');
}

/**
 * Decodes a token, or returns null if it is not one.
 *
 * Null rather than a throw, because the callers are query parameters and the
 * right answer for a malformed cursor is "no usable cursor", which the endpoint
 * turns into a 400. Accepting a bare uuid here would be worse than useless: it
 * would be the old format, which carries no position, and the caller would be back
 * to the subquery this replaced.
 */
export function decodeCursor(token: string | null | undefined): KeysetCursor | null {
  if (typeof token !== 'string' || token.length === 0) return null;
  let decoded: string;
  try {
    decoded = Buffer.from(token, 'base64url').toString('utf8');
  } catch {
    return null;
  }
  const sep = decoded.lastIndexOf('|');
  if (sep <= 0) return null;
  const createdAt = decoded.slice(0, sep);
  const id = decoded.slice(sep + 1);
  if (!TIMESTAMPTZ.test(createdAt)) return null;
  if (!UUID.test(id)) return null;
  return { createdAt, id };
}
