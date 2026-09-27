/**
 * One-off: re-record schema_migration checksums for the canonical form.
 *
 * The checksum definition changed from raw text to canonicalSql, so every stored
 * value is now stale. Each file is proven comment-only against its last commit
 * before its checksum is rewritten; anything that fails the proof is reported and
 * left alone rather than re-recorded on trust.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { Pool } from 'pg';
import { canonicalSql } from './migrate';
import { databaseUrl } from './env';

const GIT = 'C:\\Program Files\\Git\\cmd\\git.exe';
const DIR = path.join(__dirname, '..', 'migrations');
const sha = (s: string): string => createHash('sha256').update(s).digest('hex');

/** Whitespace outside literals collapses; inside a literal it is data and stays. */
const flat = (s: string): string => canonicalSql(s);

async function main(): Promise<void> {
  const pool = new Pool({ connectionString: databaseUrl(), max: 1 });
  const { rows } = await pool.query<{ filename: string }>(
    'SELECT filename FROM schema_migration ORDER BY filename',
  );

  let reRecorded = 0;
  let skipped = 0;
  for (const { filename } of rows) {
    const target = path.join(DIR, filename);
    let before: string;
    try {
      before = execFileSync(GIT, ['show', `HEAD:migrations/${filename}`], {
        encoding: 'utf8',
        maxBuffer: 1 << 24,
      });
    } catch {
      console.log(`  ${filename}: not in HEAD, skipped`);
      skipped += 1;
      continue;
    }
    const after = readFileSync(target, 'utf8');
    if (flat(before) !== flat(after)) {
      console.log(`  ${filename}: STATEMENTS DIFFER from HEAD - NOT re-recording`);
      skipped += 1;
      continue;
    }
    const next = sha(flat(after));
    const { rowCount } = await pool.query(
      'UPDATE schema_migration SET checksum = $2 WHERE filename = $1',
      [filename, next],
    );
    console.log(`  ${filename}: re-recorded (${rowCount} row, comment-only vs HEAD)`);
    reRecorded += 1;
  }

  console.log(`  files in dir: ${readdirSync(DIR).filter((f) => f.endsWith('.sql')).length}`);
  console.log(`  re-recorded:  ${reRecorded}, skipped: ${skipped}`);
  await pool.end();
}

main().then(
  () => process.exit(0),
  (e: unknown) => {
    console.log(`  failed: ${e}`);
    process.exit(1);
  },
);
