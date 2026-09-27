/**
 * The migration checksum covers statements, not prose.
 *
 * The guard refuses to run when an applied migration has changed, which is the
 * right behaviour for an edited statement and an unreasonable one for an edited
 * comment: a migration file is mostly comments, and a comment that contradicts the
 * schema beside it misleads every later reader without the database and the repo
 * disagreeing about anything.
 *
 * So the checksum is taken over a canonical form with comments dropped and
 * whitespace collapsed. These tests exist because the obvious implementation is a
 * regex, and the regex version silently mangles a `--` inside a string literal -
 * changing the statement being hashed, and making a real edit look comment-only.
 */
import { canonicalSql } from '../../scripts/migrate';

describe('canonicalSql', () => {
  it('drops a line comment', () => {
    expect(canonicalSql('SELECT 1; -- a note')).toBe('SELECT 1;');
  });

  it('drops a block comment', () => {
    expect(canonicalSql('SELECT 1; /* a note */')).toBe('SELECT 1;');
  });

  it('keeps a -- that is inside a string literal', () => {
    // The regression this guards. Read as a comment start, the tail of the
    // statement would be dropped and the checksum would cover a different query.
    expect(canonicalSql(`SELECT '--not a comment';`)).toBe(`SELECT '--not a comment';`);
  });

  it('keeps a /* that is inside a string literal', () => {
    expect(canonicalSql(`SELECT '/* not a comment */';`)).toBe(`SELECT '/* not a comment */';`);
  });

  it('does not end a string at an escaped quote', () => {
    // '' is an escaped quote. Treating it as the end of the string would flip the
    // scanner into comment mode for the rest of the file.
    expect(canonicalSql(`SELECT 'it''s -- fine';`)).toBe(`SELECT 'it''s -- fine';`);
  });

  it('preserves whitespace inside a string literal', () => {
    // There it is data, not formatting, so collapsing it would hide an edit.
    expect(canonicalSql(`SELECT 'a   b';`)).toBe(`SELECT 'a   b';`);
  });

  it('collapses whitespace outside a literal', () => {
    expect(canonicalSql('SELECT   1,\n\n   2;')).toBe('SELECT 1, 2;');
  });

  it('never lets a comment fuse the tokens around it', () => {
    // Newlines separate tokens. Dropping one entirely would make 1FROM.
    expect(canonicalSql('SELECT 1\n-- note\nFROM t')).toBe('SELECT 1 FROM t');
  });

  it('appending a trailing comment does not change the form', () => {
    // The case that made the first attempt useless: comments were dropped but the
    // newline they sat on was kept, so a note appended at the end of a file still
    // changed the hash and the guard was as obstructive as before.
    expect(canonicalSql('SELECT 1;\n')).toBe(canonicalSql('SELECT 1;\n-- a note\n'));
  });

  it('reformatting between tokens does not change the form', () => {
    expect(canonicalSql('SELECT 1;')).toBe(canonicalSql('SELECT\n\n   1;'));
  });

  it('does not normalise a space next to punctuation, and says so here', () => {
    // Documented limitation rather than a hidden one. Runs of whitespace collapse
    // to one space, but a space is still a space, so `1 ;` and `1;` differ. Making
    // that disappear means dropping whitespace next to punctuation, and the case
    // that has to be safe - a literal `'-'` next to an operator - is exactly the
    // one a naive version of that gets wrong. Refusing a comment fix is an
    // annoyance; mis-hashing a statement is a bug, so this stays conservative.
    expect(canonicalSql('SELECT 1 ;')).not.toBe(canonicalSql('SELECT 1;'));
  });

  it('changing a statement does change the form', () => {
    expect(canonicalSql('SELECT 1;')).not.toBe(canonicalSql('SELECT 2;'));
  });

  it('drops a multi-line block comment', () => {
    expect(canonicalSql('SELECT 1;\n/* one\n two */\nSELECT 2;')).toBe('SELECT 1; SELECT 2;');
  });

  it('leaves a statement with no comments alone', () => {
    expect(canonicalSql('SELECT 1;')).toBe('SELECT 1;');
  });
});
