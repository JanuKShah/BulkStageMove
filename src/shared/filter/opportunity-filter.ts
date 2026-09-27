import { BadRequestException } from '@nestjs/common';
import { UUID_RE } from '../../shared/tenancy/workspace-guard';

export const OUTCOMES = ['open', 'won', 'lost', 'abandoned'] as const;
export type Outcome = (typeof OUTCOMES)[number];

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

/**
 * Only these keys are accepted. An unrecognised filter is rejected rather than
 * ignored: a silently dropped ownerId would make a bulk job move every
 * opportunity in the workspace instead of the intended subset, and still report
 * success. Failing loudly is the only safe default here.
 */
const KNOWN_PARAMS = new Set([
  'stageId',
  'ownerId',
  'outcome',
  'minValue',
  'maxValue',
  'createdFrom',
  'createdTo',
  'limit',
  'cursor',
]);

export interface ParsedFilter {
  stageIds?: string[];
  ownerIds?: string[];
  outcome?: Outcome;
  minValue?: number;
  maxValue?: number;
  createdFrom?: Date;
  createdTo?: Date;
  limit: number;
  cursor?: string;
}

export function parseListFilter(query: Record<string, unknown>): ParsedFilter {
  for (const key of Object.keys(query)) {
    if (!KNOWN_PARAMS.has(key)) {
      throw new BadRequestException(
        `unknown filter "${key}". supported: ${[...KNOWN_PARAMS].sort().join(', ')}`,
      );
    }
    // An explicit null is not the same as an absent key. A JSON body can carry
    // {"minValue": null}, and quietly treating that as "no bound" would run a
    // job over every matching row instead of the subset the caller asked for.
    if (query[key] === null) {
      throw new BadRequestException(`${key} must not be null; omit it instead`);
    }
    // An empty list is the same hazard by another route. A body carrying
    // {"stageId": []} - a client with nothing selected - parses to undefined,
    // because the list is read as a string and an empty array yields no first
    // element. That reads as "no stage filter", and a job with no stage filter
    // matches the whole workspace: asking to move the deals in no stages at all
    // moved every deal. Refused rather than coerced, the same way null is.
    if (Array.isArray(query[key]) && query[key].length === 0) {
      throw new BadRequestException(`${key} must not be an empty list; omit it instead`);
    }
  }

  const filter: ParsedFilter = { limit: parseLimit(query['limit']) };

  const stageIds = parseUuidList('stageId', query['stageId']);
  if (stageIds) filter.stageIds = stageIds;

  const ownerIds = parseUuidList('ownerId', query['ownerId']);
  if (ownerIds) filter.ownerIds = ownerIds;

  if (query['outcome'] !== undefined) filter.outcome = parseOutcome(query['outcome']);

  const minValue = parseNumber('minValue', query['minValue']);
  if (minValue !== undefined) filter.minValue = minValue;

  const maxValue = parseNumber('maxValue', query['maxValue']);
  if (maxValue !== undefined) filter.maxValue = maxValue;

  const createdFrom = parseDate('createdFrom', query['createdFrom']);
  if (createdFrom) filter.createdFrom = createdFrom;

  const createdTo = parseDate('createdTo', query['createdTo']);
  if (createdTo) filter.createdTo = createdTo;

  if (filter.minValue !== undefined && filter.maxValue !== undefined) {
    if (filter.minValue > filter.maxValue) {
      throw new BadRequestException('minValue must be less than or equal to maxValue');
    }
  }
  if (filter.createdFrom && filter.createdTo && filter.createdFrom > filter.createdTo) {
    throw new BadRequestException('createdFrom must be earlier than or equal to createdTo');
  }

  if (query['cursor'] !== undefined)
    filter.cursor = requireValue('cursor', query['cursor'], UUID_RE);

  return filter;
}

/**
 * Reduces a value to its first scalar form.
 *
 * Numbers are stringified rather than discarded. This parser is shared by the
 * list endpoint, where every value arrives as a query string, and the bulk-move
 * endpoint, where the same filter arrives in a JSON body and numbers stay
 * numbers. Returning undefined for a non-string silently dropped `minValue`
 * from a JSON body, so the filter matched everything.
 */
function first(value: unknown): string | undefined {
  if (Array.isArray(value)) return first(value[0]);
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : undefined;
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed === '' ? undefined : trimmed;
}

/** Accepts a comma-separated list or a single value, e.g. stageId=a,b or stageId=a */
function parseUuidList(field: string, raw: unknown): string[] | undefined {
  const value = first(raw);
  if (value === undefined) return undefined;
  const parts = value
    .split(',')
    .map((p) => p.trim())
    .filter((p) => p !== '');
  if (parts.length === 0) throw new BadRequestException(`${field} must not be empty`);
  for (const part of parts) {
    if (!UUID_RE.test(part)) {
      throw new BadRequestException(`${field} must be a uuid or comma-separated uuids`);
    }
  }
  return parts;
}

function parseOutcome(raw: unknown): Outcome {
  const value = first(raw);
  if (value === undefined) throw new BadRequestException('outcome must not be empty');
  if (!(OUTCOMES as readonly string[]).includes(value)) {
    throw new BadRequestException(`outcome must be one of ${OUTCOMES.join(', ')}`);
  }
  return value as Outcome;
}

function parseNumber(field: string, raw: unknown): number | undefined {
  const value = first(raw);
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new BadRequestException(`${field} must be a number`);
  return parsed;
}

function parseDate(field: string, raw: unknown): Date | undefined {
  const value = first(raw);
  if (value === undefined) return undefined;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new BadRequestException(`${field} must be an ISO-8601 date`);
  }
  return parsed;
}

function parseLimit(raw: unknown): number {
  const value = first(raw);
  if (value === undefined) return DEFAULT_LIMIT;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new BadRequestException('limit must be a positive integer');
  }
  return Math.min(parsed, MAX_LIMIT);
}

function requireValue(field: string, raw: unknown, pattern: RegExp): string {
  const value = first(raw);
  if (value === undefined) throw new BadRequestException(`${field} must not be empty`);
  if (!pattern.test(value)) throw new BadRequestException(`${field} must be a uuid`);
  return value;
}
