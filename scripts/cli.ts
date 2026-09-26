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

async function ask(question: string): Promise<string> {
  const answer = (await rl.question(question)).trim();
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

// ---------------------------------------------------------------- rendering

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
  const entered = await askUuid('workspace id: ');
  currentWorkspace = entered;
  return true;
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

/** Renders a job's counts. Shared so the two modes cannot drift. */
function renderJobStatus(reply: Reply): void {
  print(reply, 'bulk job');
  if (reply.status >= 400) return;
  const b = reply.body as {
    status: string;
    totalMatched: number;
    failedCount: number;
    error: string | null;
    items: Record<string, number>;
  };
  const done = (b.items['completed'] ?? 0) + (b.items['failed'] ?? 0);
  console.log(`  progress: ${done}/${b.totalMatched}  status=${b.status}`);
  if (b.error) console.log(`  error:    ${b.error}`);
  const failed = b.items['failed'] ?? 0;
  if (failed > 0) {
    console.log(`\n  ${failed} record(s) did not move and are listed individually.`);
    console.log('  The rest of each batch still applied. See them with: job-failures');
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

const COMMANDS: Record<
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
        console.log(`\n  follow it with:  npm run cli -- job-status --id=${body.jobId}`);
    }
  },
  'job-status': async (args) => {
    if (!args.id) throw new Error('usage: job-status --id=<uuid>');
    renderJobStatus(await cmdJobStatus(args.id));
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
          console.log(`  track it with:  job-status  ${body.jobId}`);
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

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  if (argv.length > 0) {
    await runDirect(argv);
    return;
  }
  await interactiveMenu();
}

main()
  .catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  })
  .finally(() => rl.close());
