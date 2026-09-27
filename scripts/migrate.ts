/**
 * Migration runner. Deliberately dependency-free - uses the `pg` already in
 * package.json rather than pulling in an ORM or a migration framework.
 *
 * Applies every migrations/*.sql file that has not run yet, in filename order,
 * each inside its own transaction. Filenames are zero-padded so lexical order
 * is execution order.
 */
import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { Pool } from 'pg';
import { databaseUrl } from './env';

const MIGRATIONS_DIR = path.join(__dirname, '..', 'migrations');

/**
 * The SQL in a canonical form, for checksumming.
 *
 * The guard exists to catch one thing: an applied migration whose *statements*
 * were edited, leaving the database and the repo describing different schemas.
 * Hashing the raw text catches that, but it also blocks fixing a comment that
 * contradicts the schema next to it - and a migration file is mostly comments, and
 * a wrong one misleads the next reader about the design rather than breaking
 * anything.
 *
 * So the checksum covers the statements and not the prose, which means comments
 * are dropped *and* whitespace is canonicalised. Both are needed: dropping a
 * comment but keeping the newline it sat on still changes the hash, so merely
 * reformatting or appending a note would be refused and the guard would be as
 * annoying as before while looking deliberate.
 *
 * Whitespace outside a string literal collapses to a single space, never to
 * nothing - newlines separate tokens, and `SELECT 1\nFROM t` must not become
 * `SELECT 1FROM t`. Inside a literal, whitespace is preserved exactly, because
 * there it is data.
 *
 * A state machine rather than a regex, because a `--` inside a string literal is
 * not a comment, and treating it as one would change the statement being hashed.
 */
export function canonicalSql(sql: string): string {
  let out = '';
  let i = 0;
  let inLine = false;
  let inBlock = false;
  let inString = false;
  // A comment must not let the tokens around it fuse, so it leaves a separator.
  // Tracked separately from `out` so a trailing one is not emitted.
  let pendingSpace = false;

  const push = (text: string): void => {
    if (pendingSpace && out !== '') out += ' ';
    pendingSpace = false;
    out += text;
  };

  while (i < sql.length) {
    const two = sql.slice(i, i + 2);
    const ch = sql[i]!;

    if (inLine) {
      if (ch === '\n') inLine = false;
      i += 1;
    } else if (inBlock) {
      if (two === '*/') {
        inBlock = false;
        i += 2;
      } else {
        i += 1;
      }
    } else if (inString) {
      if (ch === "'") {
        // '' is an escaped quote, not the end of the string.
        if (two === "''") {
          push("''");
          i += 2;
        } else {
          inString = false;
          push("'");
          i += 1;
        }
      } else {
        push(ch);
        i += 1;
      }
    } else if (two === '--') {
      inLine = true;
      pendingSpace = true;
      i += 2;
    } else if (two === '/*') {
      inBlock = true;
      pendingSpace = true;
      i += 2;
    } else if (ch === "'") {
      inString = true;
      push("'");
      i += 1;
    } else if (/\s/.test(ch)) {
      pendingSpace = true;
      i += 1;
    } else {
      push(ch);
      i += 1;
    }
  }
  return out;
}

const sha256 = (text: string): string => createHash('sha256').update(text).digest('hex');

async function main(): Promise<void> {
  const connectionString = databaseUrl();

  const pool = new Pool({ connectionString, max: 1 });

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_migration (
        filename   text PRIMARY KEY,
        checksum   text        NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);

    const { rows } = await pool.query<{ filename: string; checksum: string }>(
      'SELECT filename, checksum FROM schema_migration',
    );
    const applied = new Map(rows.map((r) => [r.filename, r.checksum]));

    const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();

    if (files.length === 0) {
      console.log('no migrations found');
      return;
    }

    let ran = 0;
    for (const filename of files) {
      const sql = await readFile(path.join(MIGRATIONS_DIR, filename), 'utf8');
      const checksum = sha256(canonicalSql(sql));
      const previous = applied.get(filename);

      if (previous) {
        if (previous === checksum) continue;

        // Checksums recorded before comments were excluded hashed the raw text.
        // Re-record those, but only where the raw text is byte-identical to what
        // was applied - so this upgrades the record and never blesses a statement
        // that actually changed. Anything else is still a divergence.
        if (previous === sha256(sql)) {
          await pool.query('UPDATE schema_migration SET checksum = $2 WHERE filename = $1', [
            filename,
            checksum,
          ]);
          console.log(`${filename}: recorded checksum now covers statements only`);
          continue;
        }

        // Editing an applied migration's statements means the database and the
        // repo have diverged. Surface it instead of silently drifting.
        throw new Error(
          `${filename} has changed since it was applied. ` +
            'Add a new migration rather than editing this one.',
        );
      }

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query('INSERT INTO schema_migration (filename, checksum) VALUES ($1, $2)', [
          filename,
          checksum,
        ]);
        await client.query('COMMIT');
        console.log(`applied ${filename}`);
        ran++;
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(`${filename} failed: ${(error as Error).message}`, { cause: error });
      } finally {
        client.release();
      }
    }

    console.log(ran === 0 ? 'already up to date' : `applied ${ran} migration(s)`);
  } finally {
    await pool.end();
  }
}

// Only when run as a script. canonicalSql is imported by the unit tests and by
// the checksum tooling, and running the migrator as a side effect of importing it
// would have those fail for a reason that has nothing to do with what they check.
if (require.main === module) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
