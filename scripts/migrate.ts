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

const MIGRATIONS_DIR = path.join(__dirname, '..', 'migrations');

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set. Copy .env.example to .env first.');
  }

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
      const checksum = createHash('sha256').update(sql).digest('hex');
      const previous = applied.get(filename);

      if (previous) {
        if (previous !== checksum) {
          // Editing an applied migration means the database and the repo have
          // diverged. Surface it instead of silently drifting.
          throw new Error(
            `${filename} has changed since it was applied. ` +
              'Add a new migration rather than editing this one.',
          );
        }
        continue;
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

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
