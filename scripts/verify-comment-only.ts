/**
 * Prove a migration edit is comment-only.
 *
 * Guards the one-off re-record of an applied migration's checksum: the claim
 * being made is "the statements are unchanged, only the prose moved", and that
 * should be checked rather than asserted.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { canonicalSql } from './migrate';

const GIT = 'C:\\Program Files\\Git\\cmd\\git.exe';
const file = process.argv[2] ?? 'migrations/0001_schema.sql';

const before = execFileSync(GIT, ['show', `HEAD:${file}`], {
  encoding: 'utf8',
  maxBuffer: 1 << 24,
});
const after = readFileSync(file, 'utf8');

const h = (s: string): string => createHash('sha256').update(canonicalSql(s)).digest('hex');
const raw = (s: string): string => createHash('sha256').update(s).digest('hex');

/**
 * canonicalSql already drops comments and collapses whitespace outside string
 * literals, so it is the comparison directly. No further normalising here: a
 * second collapse would also flatten whitespace inside a literal, which is data,
 * and would hide exactly the kind of edit this is meant to catch.
 */
const flat = (s: string): string => canonicalSql(s);

const sameStatements = flat(before) === flat(after);
console.log(`  file:                ${file}`);
console.log(`  raw text changed:    ${raw(before) !== raw(after)}`);
console.log(`  statements changed:  ${!sameStatements}`);
if (!sameStatements) {
  const a = flat(before);
  const b = flat(after);
  let i = 0;
  while (i < a.length && a[i] === b[i]) i += 1;
  console.log(`  first divergence at: ${i}`);
  console.log(`    before: ...${a.slice(Math.max(0, i - 40), i + 60)}`);
  console.log(`    after:  ...${b.slice(Math.max(0, i - 40), i + 60)}`);
}
console.log(
  `  verdict:             ${sameStatements ? 'COMMENT-ONLY, safe to re-record' : 'STATEMENTS CHANGED, do not re-record'}`,
);
process.exit(sameStatements ? 0 : 1);
