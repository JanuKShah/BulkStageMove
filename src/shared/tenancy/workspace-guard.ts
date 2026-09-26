import { BadRequestException } from '@nestjs/common';

export const WORKSPACE_HEADER = 'x-workspace-id';

export const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Tenant scoping helpers. Not injectable - these are pure functions, so there is
 * nothing to inject and nothing to mock.
 */
export function requireWorkspaceId(headers: Record<string, string | string[] | undefined>): string {
  const raw = headers[WORKSPACE_HEADER];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (!value) throw new BadRequestException(`${WORKSPACE_HEADER} header is required`);
  return requireUuid(WORKSPACE_HEADER, value);
}

/**
 * Ids are validated before they reach Postgres. Without this a malformed value
 * becomes a uuid cast failure surfaced as a 500, which hides a client error as a
 * server fault.
 */
export function requireUuid(field: string, value: string): string {
  if (!UUID_RE.test(value)) throw new BadRequestException(`${field} must be a uuid`);
  return value;
}
