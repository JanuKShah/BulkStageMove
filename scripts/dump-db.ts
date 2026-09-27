import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import type { PoolClient } from 'pg';

/**
 * Writes a human-readable snapshot of the database.
 *
 * This connects to Postgres directly rather than going through the API on
 * purpose: the API paginates at 200 rows and only exposes what the services
 * happen to serve, so it cannot answer "what is actually in the database".
 *
 * Every row is written by default, so a dump can be checked by eye against a
 * filter that was meant to match a particular set. samplePerSection caps the
 * output when the benchmark dataset makes a full dump impractical.
 */
export interface DumpOptions {
  out: string;
  /** undefined means every row */
  samplePerSection?: number;
  databaseUrl: string;
}

/**
 * Every table, for the row-count summary at the top.
 *
 * Kept in step with the migrations by hand, which is exactly how `bulk_job_item`
 * came to be listed here long after the table was dropped: the first `count(*)`
 * against it threw, the whole dump aborted, and the CLI reported "could not write
 * db-state.txt" as though the database were down. So this list is the one place
 * that has to match the schema, and it is checked rather than trusted - see the
 * to_regclass guard in dumpDatabase.
 */
const TABLES = [
  'workspace',
  'app_user',
  'stage',
  'stage_transition_rule',
  'opportunity',
  'opportunity_transition',
  'bulk_job',
  'bulk_job_outbox',
  'bulk_job_failure',
] as const;

type Row = Record<string, unknown>;

function table(rows: Row[], columns: string[]): string {
  if (rows.length === 0) return '    (none)\n';
  const cell = (v: unknown): string => {
    if (v === null || v === undefined) return '-';
    if (typeof v === 'object') return JSON.stringify(v);
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      return String(v);
    }
    return '-';
  };
  const widths = columns.map((c) => Math.max(c.length, ...rows.map((r) => cell(r[c]).length)));
  const line = (parts: string[]): string => '    ' + parts.join('  ');
  const out = [line(columns.map((c, i) => c.padEnd(widths[i]!)))];
  out.push(line(widths.map((w) => '-'.repeat(w))));
  for (const row of rows) out.push(line(columns.map((c, i) => cell(row[c]).padEnd(widths[i]!))));
  return out.join('\n') + '\n';
}

function section(title: string, body: string): string {
  return `\n== ${title} ==\n${body}`;
}

export async function dumpDatabase(options: DumpOptions): Promise<{ file: string; bytes: number }> {
  const { Pool } = await import('pg');
  const pool = new Pool({ connectionString: options.databaseUrl, max: 1 });
  const limit = options.samplePerSection;
  const cap = (total: number): string =>
    limit === undefined || total <= limit
      ? `(${total})`
      : `(${total} total, showing ${limit} - use --sample=0 for all)`;

  const client: PoolClient = await pool.connect();

  try {
    const parts: string[] = [];
    parts.push('BulkStageMove - database state');
    parts.push(`generated : ${new Date().toISOString()}`);
    parts.push(`database  : ${options.databaseUrl.replace(/:[^:@/]*@/, ':***@')}`);
    parts.push('');

    // to_regclass rather than a bare count(*), so a table listed above that the
    // migrations have since dropped is reported rather than thrown on. One missing
    // name used to abort the whole dump, which is how a dead table went unnoticed
    // for so long: the only symptom was a generic "could not write db-state.txt".
    const countRows: Row[] = [];
    const missing: string[] = [];
    for (const t of TABLES) {
      const present = await client.query<{ ok: string | null }>(
        'SELECT to_regclass($1)::text AS ok',
        [t],
      );
      if (present.rows[0]!.ok === null) {
        missing.push(t);
        countRows.push({ table: t, rows: 'table does not exist' });
        continue;
      }
      const r = await client.query<{ n: string }>(`SELECT count(*)::text AS n FROM ${t}`);
      countRows.push({ table: t, rows: r.rows[0]!.n });
    }
    parts.push(section('ROW COUNTS', table(countRows, ['table', 'rows'])));
    if (missing.length > 0) {
      // Said out loud, because a silently short table list is how a dump stops
      // being trustworthy without anyone noticing.
      parts.push(
        `\n  NOTE: listed in the dump's table list but absent from the database: ` +
          `${missing.join(', ')}. The list in scripts/dump-db.ts is out of step ` +
          `with the migrations.\n`,
      );
    }

    const workspaces = await client.query(
      'SELECT id, name, created_at FROM workspace ORDER BY created_at',
    );
    parts.push(
      section(
        `WORKSPACES (${workspaces.rows.length})`,
        table(workspaces.rows as Row[], ['id', 'name', 'created_at']),
      ),
    );

    const stages = await client.query(
      `SELECT w.name AS workspace, s.id, s.name, s.outcome
       FROM stage s JOIN workspace w ON w.id = s.workspace_id
       ORDER BY w.name, s.name`,
    );
    parts.push(
      section(
        `STAGES (${stages.rows.length})`,
        table(stages.rows as Row[], ['workspace', 'id', 'name', 'outcome']),
      ),
    );

    const users = await client.query(
      `SELECT w.name AS workspace, u.id, u.name, u.email
       FROM app_user u JOIN workspace w ON w.id = u.workspace_id
       ORDER BY w.name, u.name${limit === undefined ? '' : ` LIMIT ${limit}`}`,
    );
    const userTotal = await client.query<{ n: string }>('SELECT count(*)::text AS n FROM app_user');
    parts.push(
      section(
        `USERS ${cap(Number(userTotal.rows[0]!.n))}`,
        table(users.rows as Row[], ['workspace', 'id', 'name', 'email']),
      ),
    );

    const rules = await client.query(
      `SELECT w.name AS workspace, f.name AS from_stage, t.name AS to_stage
       FROM stage_transition_rule r
       JOIN stage f ON f.id = r.from_stage_id
       JOIN stage t ON t.id = r.to_stage_id
       JOIN workspace w ON w.id = r.workspace_id
       ORDER BY w.name, f.name, t.name${limit === undefined ? '' : ` LIMIT ${limit}`}`,
    );
    const ruleTotal = await client.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM stage_transition_rule',
    );
    parts.push(
      section(
        `TRANSITION RULES ${cap(Number(ruleTotal.rows[0]!.n))}`,
        table(rules.rows as Row[], ['workspace', 'from_stage', 'to_stage']),
      ),
    );

    const opps = await client.query(
      `SELECT w.name AS workspace, o.id, o.name, s.name AS stage, s.outcome, o.value,
              u.name AS owner, o.created_at
       FROM opportunity o
       JOIN workspace w ON w.id = o.workspace_id
       JOIN stage s ON s.id = o.stage_id
       LEFT JOIN app_user u ON u.id = o.owner_id
       ORDER BY w.name, o.created_at DESC, o.id${limit === undefined ? '' : ` LIMIT ${limit}`}`,
    );
    const oppTotal = await client.query<{ n: string }>(
      'SELECT count(*)::text AS n FROM opportunity',
    );
    parts.push(
      section(
        `OPPORTUNITIES ${cap(Number(oppTotal.rows[0]!.n))}`,
        table(opps.rows as Row[], [
          'workspace',
          'id',
          'name',
          'stage',
          'outcome',
          'value',
          'owner',
          'created_at',
        ]),
      ),
    );

    const span = await client.query(
      `SELECT min(created_at)::text AS oldest, max(created_at)::text AS newest FROM opportunity`,
    );
    parts.push(section('HISTORY SPAN', table([(span.rows[0] ?? {}) as Row], ['oldest', 'newest'])));

    const content = parts.join('\n');
    const target = path.resolve(options.out);
    mkdirSync(path.dirname(target), { recursive: true });
    writeFileSync(target, content, 'utf8');
    return { file: target, bytes: Buffer.byteLength(content) };
  } finally {
    client.release();
    await pool.end();
  }
}
