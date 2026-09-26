/**
 * Recognises a unique constraint violation, by constraint name.
 *
 * Letting one escape as a 500 reports a server fault when the server knows
 * exactly what happened. The name is matched rather than any unique violation
 * because one table can carry several, and this is a predicate rather than a
 * converter because a duplicate means different things at different call sites:
 * a taken user email is a plain conflict, while a repeated idempotency key is
 * either a harmless replay or a genuine conflict depending on the request.
 */
const PG_UNIQUE_VIOLATION = '23505';

export function isUniqueViolation(error: unknown, constraint: string): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const { code, constraint: name } = error as { code?: unknown; constraint?: unknown };
  return code === PG_UNIQUE_VIOLATION && name === constraint;
}
