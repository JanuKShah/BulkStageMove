/**
 * Recognises a unique constraint violation, by constraint name.
 *
 * This exists because letting the violation escape as a 500 is wrong in both
 * directions: the caller learns nothing about what collided, and a 500 reads as
 * a server fault rather than a request that cannot be satisfied as written.
 *
 * The constraint is named rather than any unique violation, because one table
 * can carry several and mapping all of them to one response would misreport
 * what actually collided. It is also why this is a predicate and not a
 * converter: a duplicate means different things at different call sites. A
 * duplicate user email is a plain conflict, while a duplicate idempotency key
 * is either a harmless replay or a genuine conflict depending on what the
 * second request actually asked for.
 */
const PG_UNIQUE_VIOLATION = '23505';

export function isUniqueViolation(error: unknown, constraint: string): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const { code, constraint: name } = error as { code?: unknown; constraint?: unknown };
  return code === PG_UNIQUE_VIOLATION && name === constraint;
}
