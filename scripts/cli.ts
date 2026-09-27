/**
 * Console client for the API. The brief scopes out any UI, so this is how the
 * endpoints get exercised by hand.
 *
 * Two modes, one implementation:
 *   npm run cli                      interactive menu
 *   npm run cli -- opportunities list --stage=<id>   direct, scriptable
 *
 * Uses node:util parseArgs and node:readline, so it adds no dependency.
 */
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';
import { basename } from 'node:path';
import { dumpDatabase } from './dump-db';
import { databaseUrl } from './env';

const BASE = {
  workspace: process.env.WORKSPACE_URL ?? 'http://localhost:3001',
  user: process.env.USER_URL ?? 'http://localhost:3002',
  stage: process.env.STAGE_URL ?? 'http://localhost:3003',
  opportunity: process.env.OPPORTUNITY_URL ?? 'http://localhost:3004',
  transition: process.env.TRANSITION_URL ?? 'http://localhost:3005',
} as const;

type Service = keyof typeof BASE;

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

let currentWorkspace: string | null = null;

interface Reply<T = unknown> {
  status: number;
  body: T;
}

async function request<T = unknown>(
  service: Service,
  path: string,
  init: RequestInit & { workspaceId?: string | null } = {},
): Promise<Reply<T>> {
  const { workspaceId, ...rest } = init;
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (workspaceId !== null) headers['x-workspace-id'] = workspaceId ?? currentWorkspace ?? '';

  const response = await fetch(`${BASE[service]}${path}`, {
    ...rest,
    headers: { ...headers, ...(rest.headers ?? {}) },
  });
  const text = await response.text();
  return { status: response.status, body: (text === '' ? null : JSON.parse(text)) as T };
}

const ok = (r: Reply): boolean => r.status < 400;

function print(reply: Reply, label = ''): void {
  if (label) console.log(`\n--- ${label} ---`);
  if (reply.status >= 400) {
    const body = reply.body as { message?: string } | null;
    const message =
      body && typeof body.message === 'string' ? body.message : JSON.stringify(reply.body);
    console.log(`  ${reply.status}  ${message}`);
    return;
  }
  console.log(`  ${reply.status} ok`);
  console.log(indent(JSON.stringify(reply.body, null, 2)));
}

const indent = (text: string): string =>
  text
    .split('\n')
    .map((l) => `  ${l}`)
    .join('\n');

// ---------------------------------------------------------------- prompting

const rl = createInterface({ input: stdin, output: stdout });

/**
 * Asks one question and returns the trimmed answer.
 *
 * The trailing space is added here rather than at each call site. Half the
 * prompts in this file ended with one and half did not, so typing an answer ran
 * straight into the question text on some and not others, and there was no way to
 * tell from reading a call site whether it had been handled. Normalising in one
 * place means a new prompt cannot get it wrong.
 *
 * Trailing whitespace is stripped first, so a call site that already supplies a
 * space does not end up with two.
 */
async function ask(question: string): Promise<string> {
  const answer = (await rl.question(question.trimEnd() + ' ')).trim();
  return answer;
}

async function askUuid(question: string): Promise<string> {
  for (;;) {
    const value = await ask(question);
    if (UUID_RE.test(value)) return value;
    console.log('  not a uuid - expected 8-4-4-4-12 hex characters');
  }
}

async function askOptional(question: string): Promise<string | undefined> {
  const value = await ask(`${question} (blank to skip) `);
  return value === '' ? undefined : value;
}

async function askNumber(question: string): Promise<number | undefined> {
  const value = await askOptional(question);
  if (value === undefined) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * Every workspace in the database, read from Postgres rather than the API.
 *
 * There is deliberately no endpoint that enumerates workspaces: a caller must not
 * be able to discover other tenants' identities by asking. That reasoning is about
 * the *API*, though, and does not extend to a local developer tool that already
 * holds the database credentials — `dump-db` reads every table in here for the
 * same reason. So this lists what is in the database and adds no route.
 *
 * Returns [] rather than throwing when the database is unreachable, because the
 * caller may already have a workspace id from a flag and not need this at all.
 */
async function listWorkspaces(): Promise<Array<{ id: string; name: string; count: number }>> {
  try {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: databaseUrl(), max: 1 });
    try {
      const { rows } = await pool.query<{ id: string; name: string; count: string }>(
        `SELECT w.id, w.name, count(o.id)::text AS count
           FROM workspace w
           LEFT JOIN opportunity o ON o.workspace_id = w.id
          GROUP BY w.id, w.name
          ORDER BY w.created_at`,
      );
      return rows.map((r) => ({ id: r.id, name: r.name, count: Number(r.count) }));
    } finally {
      await pool.end();
    }
  } catch {
    return [];
  }
}

/**
 * Shows the workspaces that exist, so an interactive user can pick one by number
 * instead of copying a uuid out of a terminal.
 *
 * Only shown in interactive mode with no workspace already resolved. Once
 * --workspace or WORKSPACE_ID has named a tenant, listing the others is noise, and
 * in a scripted run it would be output nobody asked for.
 */
async function showWorkspaces(): Promise<void> {
  const found = await listWorkspaces();
  if (found.length === 0) {
    console.log('  no workspaces in the database. run "npm run seed" to create one.');
    return;
  }
  console.log(`\n  ${found.length} workspace(s):`);
  found.forEach((w, i) => {
    console.log(`    ${i + 1}. ${w.name}  (${w.count} opportunities)  ${w.id}`);
  });
  console.log('  type a number, or paste a uuid.');
}

/** ---------------------------------------------------------------- rendering */

/**
 * The workspace id is always supplied by the caller - via --workspace, the
 * WORKSPACE_ID environment variable, or a prompt in interactive mode.
 *
 * It is deliberately never discovered. There is no endpoint that enumerates
 * workspaces, because that would hand any caller the identity of every tenant
 * on the platform. A real client has its own workspace id configured; it does
 * not go looking for other tenants. Run `npm run seed` to print the ids.
 */
async function resolveWorkspaceId(interactive: boolean): Promise<boolean> {
  const supplied = currentWorkspace ?? process.env.WORKSPACE_ID ?? null;
  if (supplied) {
    if (!UUID_RE.test(supplied)) {
      console.error(`workspace id is not a uuid: ${supplied}`);
      return false;
    }
    currentWorkspace = supplied;
    return true;
  }
  if (!interactive) {
    console.error(
      'a workspace id is required.\n' +
        '  pass --workspace=<uuid> or set WORKSPACE_ID.\n' +
        '  run "npm run seed" to print the ids it created.',
    );
    return false;
  }
  const entered = await askWorkspaceChoice();
  currentWorkspace = entered;
  return true;
}

/**
 * Prompts for a workspace, offering the ones that exist by number.
 *
 * Accepting a number is the point: a uuid is 36 characters that have to be copied
 * by hand, and a typo is only caught by the API rejecting it. A number cannot be
 * mistyped, and the uuid is printed beside it either way.
 */
async function askWorkspaceChoice(): Promise<string> {
  await showWorkspaces();
  for (;;) {
    const answer = await ask('workspace: ');
    const trimmed = answer.trim();
    if (trimmed === '') continue;
    if (/^\d+$/.test(trimmed)) {
      const found = await listWorkspaces();
      const picked = found[Number(trimmed) - 1];
      if (!picked) {
        console.log(`  there is no workspace ${trimmed} - pick 1-${found.length}`);
        continue;
      }
      return picked.id;
    }
    if (UUID_RE.test(trimmed)) return trimmed;
    console.log('  not a number or a uuid');
  }
}

// ---------------------------------------------------------------- commands

interface Row {
  [key: string]: string | number | null | undefined;
}

const cell = (value: Row[string]): string =>
  value === null || value === undefined ? '' : String(value);

function table(rows: Row[], columns: string[]): void {
  if (rows.length === 0) {
    console.log('  (none)');
    return;
  }
  const widths = columns.map((c) => Math.max(c.length, ...rows.map((r) => cell(r[c]).length)));
  console.log('  ' + columns.map((c, i) => c.padEnd(widths[i]!)).join('  '));
  console.log('  ' + widths.map((w) => '-'.repeat(w)).join('  '));
  for (const row of rows) {
    console.log('  ' + columns.map((c, i) => cell(row[c]).padEnd(widths[i]!)).join('  '));
  }
}

async function cmdHealth(): Promise<Reply[]> {
  const results: Reply[] = [];
  for (const [name, url] of Object.entries(BASE)) {
    try {
      const response = await fetch(`${url}/health/ready`);
      const body = await response.text();
      results.push({ status: response.status, body: `${name}: ${response.status} ${body}` });
    } catch (error) {
      results.push({ status: 0, body: `${name}: unreachable (${(error as Error).message})` });
    }
  }
  return results;
}

async function cmdJobFailures(jobId: string, limit?: number): Promise<Reply> {
  const qs = limit ? `?limit=${limit}` : '';
  return request('transition', `/bulk-moves/${jobId}/failures${qs}`);
}

async function cmdJobBatches(jobId: string): Promise<Reply> {
  return request('transition', `/bulk-moves/${jobId}/batches`);
}

async function cmdJobStatus(jobId: string): Promise<Reply> {
  return request('transition', `/bulk-moves/${jobId}`);
}

/**
 * Puts back the batches that ran out of attempts.
 *
 * Nothing does this on its own, so it is a command rather than a flag: a
 * deliberate act, on a job that has already reported itself finished.
 */
async function cmdJobRetry(jobId: string): Promise<Reply> {
  return request('transition', `/bulk-moves/${jobId}/retry-failed`, { method: 'POST' });
}

async function cmdJobTransitions(jobId: string, limit?: number): Promise<Reply> {
  const qs = limit ? `?limit=${limit}` : '';
  return request('transition', `/bulk-moves/${jobId}/transitions${qs}`);
}

async function cmdSubmitBulkMove(
  targetStageId: string,
  idempotencyKey: string,
  filter: Record<string, unknown>,
): Promise<Reply> {
  return request('transition', '/bulk-moves', {
    method: 'POST',
    body: JSON.stringify({ idempotencyKey, targetStageId, ...filter }),
  });
}

/** Prompts for the filter dimensions, skipping anything left blank. */
async function promptFilter(): Promise<Record<string, unknown>> {
  const filter: Record<string, unknown> = {};
  const stageId = await askOptional('  stageId (comma separated ok)');
  if (stageId) filter['stageId'] = stageId;
  const ownerId = await askOptional('  ownerId');
  if (ownerId) filter['ownerId'] = ownerId;
  const outcome = await askOptional('  outcome (open|won|lost|abandoned)');
  if (outcome) filter['outcome'] = outcome;
  const minValue = await askNumber('  minValue');
  if (minValue !== undefined) filter['minValue'] = minValue;
  const maxValue = await askNumber('  maxValue');
  if (maxValue !== undefined) filter['maxValue'] = maxValue;
  const createdFrom = await askOptional('  createdFrom (ISO date)');
  if (createdFrom) filter['createdFrom'] = createdFrom;
  const createdTo = await askOptional('  createdTo (ISO date)');
  if (createdTo) filter['createdTo'] = createdTo;
  return filter;
}

/**
 * Collects the filter flags into a body. minValue/maxValue become real JSON
 * numbers rather than strings, because that is what a UI would send, and the
 * parser is expected to handle both.
 */
function filterFromArgs(args: Record<string, string | undefined>): Record<string, unknown> {
  const filter: Record<string, unknown> = {};
  for (const key of ['stageId', 'ownerId', 'outcome', 'createdFrom', 'createdTo'] as const) {
    const value = args[key];
    if (value !== undefined) filter[key] = value;
  }
  for (const key of ['minValue', 'maxValue'] as const) {
    const value = args[key];
    if (value === undefined) continue;
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) throw new Error(`${key} must be a number, got "${value}"`);
    filter[key] = parsed;
  }
  return filter;
}

/** Renders a job's transitions. Shared so the two modes cannot drift. */
function renderJobTransitions(reply: Reply): void {
  if (reply.status >= 400) {
    print(reply, 'job transitions');
    return;
  }
  const body = reply.body as { items: Row[]; nextCursor: string | null };
  table(
    body.items.map((t) => ({
      opportunity: String(t['opportunity_id']).slice(0, 8),
      from: t['from_stage_name'] ? String(t['from_stage_name']) : '(new)',
      to: String(t['to_stage_name']),
      outcome: t['to_outcome'] ?? '',
      at: String(t['created_at']).slice(0, 19),
    })),
    ['opportunity', 'from', 'to', 'outcome', 'at'],
  );
  console.log(`  nextCursor: ${body.nextCursor ?? '(none)'}`);
}

/**
 * A job as GET /bulk-moves/:id returns it.
 *
 * The batch counts are named `batches`, not `items`. Reading `items` compiled
 * fine and threw at runtime on every call, because the field the API sends has
 * never been called that: the type was declared here rather than derived from the
 * response, so nothing checked the two against each other.
 *
 * There is no record-level progress in this shape. `batches` counts the 50 batch
 * rows and `failedCount` counts records, but the records that *succeeded* are only
 * in the database's processed_count, which the endpoint does not return. Anything
 * showing a per-record figure here would be guessing from the batch size.
 */
interface JobStatus {
  id: string;
  status: string;
  totalMatched: number;
  failedCount: number;
  error: string | null;
  snapshotInProgress: boolean;
  batches: { pending: number; running: number; completed: number; failed: number };
  deadLettered: { batches: number; records: number; reasons: string[] };
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

/** Renders a job's counts. Shared so the two modes cannot drift. */
function renderJobStatus(reply: Reply): void {
  print(reply, 'bulk job');
  if (reply.status >= 400) return;
  const b = reply.body as JobStatus;
  const settled = b.batches.completed + b.batches.failed;
  const total = b.batches.pending + b.batches.running + settled;
  console.log(
    `  batches:   ${settled}/${total} settled` +
      (b.batches.running ? `  ${b.batches.running} running` : '') +
      (b.batches.pending ? `  ${b.batches.pending} pending` : ''),
  );
  console.log(`  records:   ${b.totalMatched} matched, ${b.failedCount} failed`);
  console.log(`  status:    ${b.status}`);
  if (b.error) console.log(`  error:     ${b.error}`);
  if (b.deadLettered.batches > 0) {
    console.log(
      `  given up: ${b.deadLettered.batches} batch(es), ` +
        `${b.deadLettered.records} record(s) never moved`,
    );
    // Worth printing rather than leaving to the docs: these records are in no
    // counter anywhere, and nothing retries them unless a caller asks.
    console.log('  They are not retried automatically. To try them again: job-retry --id=<uuid>');
  }
  if (b.failedCount > 0) {
    console.log(`\n  ${b.failedCount} record(s) did not move and are listed individually.`);
    console.log('  The rest of each batch still applied. See them with: job-failures');
  }
}

/** A status is finished when nothing is left that a worker could still take. */
function isSettled(b: JobStatus): boolean {
  const terminal = b.status === 'completed' || b.status === 'failed';
  return terminal && b.batches.pending === 0 && b.batches.running === 0;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Polls a job every couple of seconds until it settles.
 *
 * Two seconds because that is the shortest interval that is still free: the sweep
 * and the relay both tick every 125 ms, so a job submitted now has usually begun
 * batching before the first poll, and by the time a caller is asking about a
 * 50,000-record job the interesting part is the drain. Polling faster would report
 * the same number more often.
 *
 * Progress is drawn in place on a terminal and appended line by line when piped,
 * so the same command is readable live and readable in a log or a CI transcript.
 *
 * The timeout is not optional. A job whose worker died mid-flight stays `running`
 * forever, and a watcher that never returns is indistinguishable from a hung one.
 */
async function watchJob(jobId: string, intervalMs: number, timeoutMs: number): Promise<void> {
  const interactive = process.stdout.isTTY === true;
  const started = Date.now();
  let line = '';

  for (;;) {
    const reply = await cmdJobStatus(jobId);
    if (reply.status === 404) {
      console.log(`  no such job in this workspace: ${jobId}`);
      return;
    }
    if (reply.status >= 400) {
      print(reply, 'bulk job');
      return;
    }

    const b = reply.body as JobStatus;
    const elapsed = (Date.now() - started) / 1000;
    // Rate is per batch, not per record: the endpoint does not return how many
    // records have moved, only how many batches have settled, so a record rate
    // here would be the batch size multiplied by guesswork.
    //
    // Suppressed for the first second. Over 0.1s the divisor is small enough that
    // the quotient is mostly the shape of the poll - it reads as hundreds per
    // second and then collapses, which looks like a stall rather than a start.
    const settled = b.batches.completed + b.batches.failed;
    const total = b.batches.pending + b.batches.running + settled;
    const rate = elapsed >= 1 ? `${(settled / elapsed).toFixed(1)} batches/s` : '  -  ';

    // While the snapshot is building there are no batches and totalMatched is
    // still zero, so a percentage would be 0/0. Say what is actually happening
    // instead of printing a bar that never moves.
    const progress = b.snapshotInProgress
      ? 'batching the filter'
      : `${settled}/${total} batches  ${b.batches.running} running  ` +
        `${b.batches.pending} pending`;

    const next =
      `  ${elapsed.toFixed(1).padStart(6)}s  ${b.status.padEnd(10)}  ${progress}  ${rate}` +
      (b.failedCount > 0 ? `  ${b.failedCount} failed` : '');

    if (interactive) {
      // Pad to the previous width so the shorter line cannot leave debris.
      line = next.padEnd(Math.max(line.length, next.length));
      process.stdout.write(`\r${line}`);
    } else {
      console.log(next);
    }

    if (isSettled(b)) {
      if (interactive) process.stdout.write('\n');
      const secs = (Date.now() - started) / 1000;
      console.log(
        `\n  ${b.status} in ${secs.toFixed(1)}s  ` +
          `${settled} batch(es), ${b.totalMatched} matched, ${b.failedCount} failed`,
      );
      if (b.error) console.log(`  error: ${b.error}`);
      if (b.deadLettered.batches > 0) {
        console.log(
          `  ${b.deadLettered.batches} batch(es) dead-lettered, ` +
            `${b.deadLettered.records} record(s) never attempted`,
        );
      }
      if (b.failedCount > 0) {
        console.log(`  see them with: job-failures --id=${jobId}`);
      }
      return;
    }

    const remaining = timeoutMs - (Date.now() - started);
    if (remaining <= 0) {
      if (interactive) process.stdout.write('\n');
      // The elapsed time, not the configured bound: at 300 ms a bound rendered to
      // whole seconds reads as "after 0s", which looks like it gave up instantly.
      const gave = ((Date.now() - started) / 1000).toFixed(1);
      console.log(
        `\n  still ${b.status} after ${gave}s, giving up.` +
          '  The job is not cancelled - it keeps running. Check it with job-status.',
      );
      return;
    }

    // Capped by what is left of the budget. Sleeping the full interval past the
    // deadline means the check above is never reached, so a timeout shorter than
    // one poll interval would silently never fire - which is the one case where
    // the caller asked for a bound and did not get one.
    await sleep(Math.min(intervalMs, remaining));
  }
}

/** Renders which records failed and why - the only actionable failure output. */
function renderJobFailures(reply: Reply): void {
  if (reply.status >= 400) {
    print(reply, 'job failures');
    return;
  }
  const rows = (reply.body as Row[]).map((f) => ({
    batch: f['batch_no'],
    opportunity: String(f['opportunity_id']).slice(0, 8),
    name: f['name'],
    from_stage: String(f['from_stage_id']).slice(0, 8),
    attempts: f['attempts'],
    error: String(f['error'] ?? '').slice(0, 60),
  }));
  table(rows, ['batch', 'opportunity', 'name', 'from_stage', 'attempts', 'error']);
}

/** Renders per-batch progress, which is the unit work is actually done in. */
function renderJobBatches(reply: Reply): void {
  if (reply.status >= 400) {
    print(reply, 'job batches');
    return;
  }
  table(
    (reply.body as Row[]).map((b) => ({
      batch: b['batch_no'],
      total: b['total'],
      completed: b['completed'],
      failed: b['failed'],
      outstanding: b['pending'],
      state:
        (b['failed'] as number) > 0 ? 'FAILED' : (b['pending'] as number) > 0 ? 'pending' : 'done',
    })),
    ['batch', 'total', 'completed', 'failed', 'outstanding', 'state'],
  );
}

async function cmdListOpportunities(args: Record<string, string | undefined>): Promise<Reply> {
  const params = new URLSearchParams();
  const map: Record<string, string> = {
    stageId: 'stageId',
    ownerId: 'ownerId',
    outcome: 'outcome',
    minValue: 'minValue',
    maxValue: 'maxValue',
    createdFrom: 'createdFrom',
    createdTo: 'createdTo',
    limit: 'limit',
    cursor: 'cursor',
  };
  for (const [flag, param] of Object.entries(map)) {
    const value = args[flag];
    if (value !== undefined) params.set(param, value);
  }
  const query = params.toString();
  const reply = await request<{ items: Row[]; nextCursor: string | null }>(
    'opportunity',
    `/opportunities${query ? `?${query}` : ''}`,
  );
  if (ok(reply)) {
    table(
      reply.body.items.map((i) => ({
        id: i.id,
        stage: String(i.stage_id).slice(0, 8),
        name: i.name,
        value: i.value,
        owner: i.owner_id ? String(i.owner_id).slice(0, 8) : '-',
      })),
      ['id', 'stage', 'name', 'value', 'owner'],
    );
    console.log(`  nextCursor: ${reply.body.nextCursor ?? '(none)'}`);
  }
  return reply;
}

async function cmdStages(): Promise<Reply> {
  const reply = await request<Row[]>('stage', '/stages');
  if (ok(reply)) {
    table(
      reply.body.map((s) => ({
        id: s.id,
        name: s.name,
        outcome: s.outcome,
      })),
      ['id', 'name', 'outcome'],
    );
  }
  return reply;
}

async function cmdUsers(): Promise<Reply> {
  const reply = await request<Row[]>('user', '/users');
  if (ok(reply)) {
    table(
      reply.body.map((u) => ({ id: u.id, name: u.name, email: u.email ?? '-' })),
      ['id', 'name', 'email'],
    );
  }
  return reply;
}

async function cmdCanMove(from: string, to: string): Promise<Reply> {
  return request('stage', '/stages/can-move', {
    method: 'POST',
    body: JSON.stringify({ from, to }),
  });
}

async function cmdCreateOpportunity(
  stageId: string,
  name: string,
  value?: number,
  ownerId?: string,
): Promise<Reply> {
  return request('opportunity', '/opportunities', {
    method: 'POST',
    body: JSON.stringify({ stageId, name, value, ownerId }),
  });
}

async function cmdMove(id: string, toStageId: string): Promise<Reply> {
  return request('opportunity', `/opportunities/${id}/move`, {
    method: 'POST',
    body: JSON.stringify({ toStageId }),
  });
}

async function cmdTransitions(id: string): Promise<Reply> {
  const reply = await request<Row[]>('opportunity', `/opportunities/${id}/transitions`);
  if (ok(reply)) {
    table(
      reply.body.map((t) => ({
        at: String(t.created_at).slice(0, 19),
        from: t.from_stage_id ? String(t.from_stage_id).slice(0, 8) : '(new)',
        to: String(t.to_stage_id).slice(0, 8),
      })),
      ['at', 'from', 'to'],
    );
  }
  return reply;
}

// ---------------------------------------------------------------- direct mode

type CommandResult = Reply | void;

// Exported so a test can assert the command surface rather than the README
// asserting it. The README names one of these as the example of the
// non-interactive interface, and documentation naming a command nobody runs rots
// without anything failing.
export const COMMANDS: Record<
  string,
  (args: Record<string, string | undefined>) => Promise<CommandResult>
> = {
  health: async () => {
    for (const r of await cmdHealth()) console.log(`  ${String(r.body)}`);
  },
  'workspace-info': async () => {
    const id = currentWorkspace;
    if (!id) throw new Error('a workspace id is required');
    return request('workspace', `/workspaces/${id}`, { workspaceId: null });
  },
  'bulk-move': async (args) => {
    if (!args.to)
      throw new Error(
        'usage: bulk-move --to=<stageId> [--stageId=..] [--outcome=..] [--minValue=..]',
      );
    const key = args.key ?? randomUUID();
    console.log(`  idempotencyKey: ${key}`);
    console.log('  reuse it to retry this exact submission safely');
    const result = await cmdSubmitBulkMove(args.to, key, filterFromArgs(args));
    print(result, 'bulk-moves');
    if (result.status < 400) {
      const body = result.body as { jobId?: string };
      if (body.jobId)
        console.log(`\n  watch it with:  npm run cli -- job-watch --id=${body.jobId}`);
    }
  },
  'job-status': async (args) => {
    if (!args.id) throw new Error('usage: job-status --id=<uuid>');
    renderJobStatus(await cmdJobStatus(args.id));
  },
  'job-retry': async (args) => {
    if (!args.id) throw new Error('usage: job-retry --id=<uuid>');
    const r = (await cmdJobRetry(args.id)) as unknown as {
      retriedBatches?: number;
      retriedRecords?: number;
    };
    if (r.retriedBatches) {
      console.log(`  re-queued ${r.retriedBatches} batch(es), ${r.retriedRecords} record(s).`);
      console.log(`  watch it with:  job-watch --id=${args.id}`);
    } else {
      console.log('  nothing to retry - no batch has run out of attempts.');
    }
  },
  'job-watch': async (args) => {
    if (!args.id) throw new Error('usage: job-watch --id=<uuid> [--interval=ms] [--timeout=ms]');
    const interval = Number(args.interval);
    const timeout = Number(args.timeout);
    await watchJob(
      args.id,
      Number.isFinite(interval) && interval >= 250 ? interval : 2000,
      Number.isFinite(timeout) && timeout > 0 ? timeout : 300_000,
    );
  },
  'job-transitions': async (args) => {
    if (!args.id) throw new Error('usage: job-transitions --id=<uuid> [--limit=N]');
    renderJobTransitions(
      await cmdJobTransitions(args.id, args.limit ? Number(args.limit) : undefined),
    );
  },
  'job-failures': async (args) => {
    if (!args.id) throw new Error('usage: job-failures --id=<uuid> [--limit=N]');
    renderJobFailures(await cmdJobFailures(args.id, args.limit ? Number(args.limit) : undefined));
  },
  'job-batches': async (args) => {
    if (!args.id) throw new Error('usage: job-batches --id=<uuid>');
    renderJobBatches(await cmdJobBatches(args.id));
  },
  'dump-db': async (args) => {
    const out = args.out ?? 'db-state.txt';
    const sample = Number(args.sample);
    const result = await dumpDatabase({
      out,
      // no --sample, or --sample=0, means every row
      samplePerSection: Number.isFinite(sample) && sample > 0 ? sample : undefined,
      databaseUrl: databaseUrl(),
    });
    console.log(`  wrote ${result.file} (${result.bytes} bytes)`);
  },
  stages: async () => cmdStages(),
  users: async () => cmdUsers(),
  opportunities: async (args) => cmdListOpportunities(args),
  'can-move': async (args) => {
    if (!args.from || !args.to) throw new Error('usage: can-move --from=<uuid> --to=<uuid>');
    return cmdCanMove(args.from, args.to);
  },
  'create-opportunity': async (args) => {
    if (!args.stage || !args.name) {
      throw new Error(
        'usage: create-opportunity --stage=<uuid> --name=<str> [--value=<n>] [--owner=<uuid>]',
      );
    }
    return cmdCreateOpportunity(
      args.stage,
      args.name,
      args.value ? Number(args.value) : undefined,
      args.owner,
    );
  },
  move: async (args) => {
    if (!args.id || !args.to) throw new Error('usage: move --id=<uuid> --to=<uuid>');
    return cmdMove(args.id, args.to);
  },
  transitions: async (args) => {
    if (!args.id) throw new Error('usage: transitions --id=<uuid>');
    return cmdTransitions(args.id);
  },
};

const OPTION_SPEC = {
  workspace: { type: 'string' },
  from: { type: 'string' },
  to: { type: 'string' },
  stage: { type: 'string' },
  name: { type: 'string' },
  value: { type: 'string' },
  owner: { type: 'string' },
  stageId: { type: 'string' },
  ownerId: { type: 'string' },
  outcome: { type: 'string' },
  minValue: { type: 'string' },
  maxValue: { type: 'string' },
  createdFrom: { type: 'string' },
  createdTo: { type: 'string' },
  limit: { type: 'string' },
  cursor: { type: 'string' },
  out: { type: 'string' },
  sample: { type: 'string' },
  id: { type: 'string' },
  key: { type: 'string' },
  interval: { type: 'string' },
  timeout: { type: 'string' },
} as const;

async function runDirect(argv: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args: argv,
    options: OPTION_SPEC,
    allowPositionals: true,
    strict: true,
  });
  // positionals[0] is the command: `npm run cli -- <command>` does not pass the
  // script name through, so there is nothing to skip here.
  const [name, ...rest] = positionals;
  if (!name) {
    console.log('usage: npm run cli -- <command> [flags]');
    console.log(`commands: ${Object.keys(COMMANDS).join(', ')}`);
    return;
  }
  const handler = COMMANDS[name];
  if (!handler) {
    console.log(`unknown command "${name}". try one of: ${Object.keys(COMMANDS).join(', ')}`);
    return;
  }
  // These operate across all workspaces, so they need no workspace id.
  const NO_WORKSPACE = new Set(['health', 'dump-db']);
  if (name !== 'health' && values.workspace) {
    currentWorkspace = values.workspace;
  }
  if (!NO_WORKSPACE.has(name) && !(await resolveWorkspaceId(false))) {
    return;
  }
  void rest;
  const result = await handler(values);
  if (result && typeof result === 'object' && 'status' in result) {
    print(result, name);
  }
}

// ---------------------------------------------------------------- interactive

interface MenuEntry {
  key: string;
  label: string;
  run: () => Promise<void>;
}

async function interactiveMenu(): Promise<void> {
  console.log('\n=== BulkStageMove ===');
  if (!(await resolveWorkspaceId(true))) {
    console.log('\ncannot continue without a workspace id.');
    return;
  }
  console.log(`workspace: ${currentWorkspace}`);

  const entries: MenuEntry[] = [
    {
      key: 'health',
      label: 'health check all services',
      run: async () => {
        for (const r of await cmdHealth()) console.log(`  ${String(r.body)}`);
      },
    },
    {
      key: 'ws',
      label: 'switch workspace',
      run: async () => {
        if (await resolveWorkspaceId(true)) console.log(`workspace: ${currentWorkspace}`);
      },
    },
    {
      key: 'dump',
      label: 'dump database state to a file',
      run: async () => {
        const out = await askOptional('  output path');
        const sample = await askNumber('  max rows per section (blank or 0 = all)');
        const result = await dumpDatabase({
          out: out ?? 'db-state.txt',
          samplePerSection: sample && sample > 0 ? sample : undefined,
          databaseUrl: databaseUrl(),
        });
        console.log(`\n  wrote ${result.file} (${result.bytes} bytes)`);
      },
    },
    {
      key: 'stages',
      label: 'list stages',
      run: async () => void (await cmdStages()),
    },
    {
      key: 'users',
      label: 'list users',
      run: async () => void (await cmdUsers()),
    },
    {
      key: 'list',
      label: 'list opportunities (all filters)',
      run: async () => {
        const stageId = await askOptional('  stageId (comma separated ok)');
        const ownerId = await askOptional('  ownerId');
        const outcome = await askOptional('  outcome (open|won|lost|abandoned)');
        const minValue = await askNumber('  minValue');
        const maxValue = await askNumber('  maxValue');
        const createdFrom = await askOptional('  createdFrom (ISO date)');
        const createdTo = await askOptional('  createdTo (ISO date)');
        const limit = await askNumber('  limit');
        const args: Record<string, string | undefined> = {
          stageId,
          ownerId,
          outcome,
          minValue: minValue?.toString(),
          maxValue: maxValue?.toString(),
          createdFrom,
          createdTo,
          limit: (limit ?? 20).toString(),
        };
        void (await cmdListOpportunities(args));
      },
    },
    {
      key: 'create',
      label: 'create an opportunity',
      run: async () => {
        const stageId = await askUuid('  stageId: ');
        const name = await ask('  name: ');
        const value = await askNumber('  value');
        const ownerId = await askOptional('  ownerId');
        void (await cmdCreateOpportunity(
          stageId,
          name,
          value,
          ownerId && UUID_RE.test(ownerId) ? ownerId : undefined,
        ));
      },
    },
    {
      key: 'move',
      label: 'move an opportunity to another stage',
      run: async () => {
        const id = await askUuid('  opportunity id: ');
        const toStageId = await askUuid('  to stageId: ');
        void (await cmdMove(id, toStageId));
      },
    },
    {
      key: 'canmove',
      label: 'check whether a move is permitted',
      run: async () => {
        const from = await askUuid('  from stageId: ');
        const to = await askUuid('  to stageId: ');
        void (await cmdCanMove(from, to));
      },
    },
    {
      key: 'bulk',
      label: 'submit a bulk stage move',
      run: async () => {
        const targetStageId = await askUuid('  to stageId: ');
        console.log('  filter - leave blank to match every opportunity in the workspace');
        const filter = await promptFilter();
        const key = await askOptional('  idempotency key (blank to generate)');
        const result = await cmdSubmitBulkMove(targetStageId, key ?? randomUUID(), filter);
        print(result, 'bulk-moves');
        const body = result.body as { jobId?: string; replay?: boolean } | null;
        if (result.status < 400 && body?.jobId) {
          console.log(`  watch it with:  job-watch  ${body.jobId}`);
        }
      },
    },
    {
      key: 'jobstatus',
      label: 'check bulk job status',
      run: async () => {
        renderJobStatus(await cmdJobStatus(await askUuid('  job id: ')));
      },
    },
    {
      key: 'jobwatch',
      label: 'watch a bulk job until it finishes',
      run: async () => {
        const id = await askUuid('  job id: ');
        const raw = await askOptional('  poll interval ms (blank = 2000)');
        const interval = raw ? Number(raw) : 2000;
        await watchJob(id, Number.isFinite(interval) && interval >= 250 ? interval : 2000, 300_000);
      },
    },
    {
      key: 'jobtrans',
      label: 'view a bulk job transitions',
      run: async () => {
        const id = await askUuid('  job id: ');
        const limit = await askNumber('  limit (blank = default)');
        renderJobTransitions(await cmdJobTransitions(id, limit));
      },
    },
    {
      key: 'jobbatches',
      label: 'view bulk job batch progress',
      run: async () => {
        renderJobBatches(await cmdJobBatches(await askUuid('  job id: ')));
      },
    },
    {
      key: 'jobfails',
      label: 'view why a bulk job failed',
      run: async () => {
        const id = await askUuid('  job id: ');
        const limit = await askNumber('  max rows (blank = default)');
        renderJobFailures(await cmdJobFailures(id, limit));
      },
    },
    {
      key: 'jobretry',
      label: 'retry the batches a bulk job gave up on',
      run: async () => {
        const id = await askUuid('  job id: ');
        const r = (await cmdJobRetry(id)) as unknown as {
          retriedBatches?: number;
          retriedRecords?: number;
        };
        if (r.retriedBatches) {
          console.log(`  re-queued ${r.retriedBatches} batch(es), ${r.retriedRecords} record(s).`);
          console.log(`  watch it with:  job-watch --id=${id}`);
        } else {
          console.log('  nothing to retry - no batch has run out of attempts.');
        }
      },
    },
    {
      key: 'history',
      label: 'view an opportunity transition history',
      run: async () => {
        const id = await askUuid('  opportunity id: ');
        void (await cmdTransitions(id));
      },
    },
  ];

  for (;;) {
    console.log('\n--- menu ---');
    entries.forEach((e, i) => console.log(`  ${i + 1}. ${e.label}`));
    console.log('  0. quit');
    const choice = (await ask('\nselect')).trim();
    if (choice === '0' || choice === 'q') return;
    const entry = entries[Number(choice) - 1];
    if (!entry) {
      console.log('  invalid choice');
      continue;
    }
    try {
      await entry.run();
    } catch (error) {
      console.log(`  error: ${(error as Error).message}`);
    }
  }
}

// ---------------------------------------------------------------- entry

/**
 * Writes db-state.txt before the menu opens.
 *
 * Interactive only. A scripted `npm run cli -- job-status --id=...` has a caller
 * reading stdout for one value, and a multi-megabyte file appearing first would
 * corrupt anything parsing it. The menu is a human, so a snapshot of the database
 * as it was when they sat down is worth the second it costs.
 *
 * Capped at 200 rows per section, because the benchmark dataset holds 500,000
 * opportunities and an uncapped dump would be hundreds of megabytes of text no one
 * is going to read. Row counts are always exact, so the cap is visible rather than
 * silent. The uncapped dump stays one flag away: the dump-db command.
 *
 * Failure is reported and then ignored. A missing snapshot is worth mentioning
 * once; it is not worth refusing to open a menu over.
 */
async function dumpOnLaunch(): Promise<void> {
  try {
    const result = await dumpDatabase({
      out: 'db-state.txt',
      samplePerSection: 200,
      databaseUrl: databaseUrl(),
    });
    console.log(`  database snapshot: ${basename(result.file)} (${result.bytes} bytes)`);
    console.log('    run "npm run dump" for every row, or diff before/after a bulk move.');
  } catch (error) {
    console.log(`  could not write db-state.txt: ${(error as Error).message}`);
    console.log('    is postgres up?  "npm run up"');
  }
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  if (argv.length > 0) {
    await runDirect(argv);
    return;
  }
  await dumpOnLaunch();
  await interactiveMenu();
}

// Only when run as a script. COMMANDS is imported by a test that checks the
// command surface, and running the CLI as a side effect of importing it would
// have that test start a readline prompt and make requests it never asked for.
if (require.main === module) {
  main()
    .catch((error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exitCode = 1;
    })
    .finally(() => rl.close());
}
