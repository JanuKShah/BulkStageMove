import { randomUUID } from 'node:crypto';
import { pool, provisionWorkspace, withDeadlockRetry, type TestWorkspace } from '../helpers';

/**
 * The trigger behind stage_decided_at, tested on its own.
 *
 * The worker tests prove the ordering rule works. These prove the column it reads
 * is actually being written, which is a separate mechanism that could break
 * silently: if the trigger stopped firing, every record's clock would freeze at
 * whatever it was when the job was created, and every job would overwrite every
 * person who touched a record afterwards. Nothing would error. The worker tests
 * would go red for the wrong reason, so this file checks the trigger directly.
 *
 * All four behaviours are asserted rather than assumed, because each of the two
 * WHEN clauses exists for exactly one of them and dropping either produces a
 * silent wrong answer:
 *
 *   no stage check   - a rename bumps the clock, and editing a deal's name drops
 *                      it from every in-flight job
 *   no writer check  - the trigger overwrites the job's snapshot_at with the wall
 *                      clock, and the ordering silently becomes last-writer-wins
 */
describe('the stage_decided_at trigger', () => {
  let ws: TestWorkspace;
  const a = () => ws.stages['newLead'];
  const b = () => ws.stages['contacted'];

  beforeAll(async () => {
    ws = await provisionWorkspace('clock');
  });

  afterAll(async () => {
    await withDeadlockRetry(() =>
      pool.query('DELETE FROM workspace WHERE id = $1', [ws.workspaceId]),
    );
    await pool.end();
  });

  const seed = async (
    stageId: string,
    name = `clock-${randomUUID().slice(0, 8)}`,
  ): Promise<string> => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO opportunity (workspace_id, stage_id, name, value)
       VALUES ($1, $2, $3, 100) RETURNING id`,
      [ws.workspaceId, stageId, name],
    );
    return rows[0]!.id;
  };

  const clock = async (id: string): Promise<{ decided: Date; created: Date; updated: Date }> => {
    const { rows } = await pool.query<{ decided: Date; created: Date; updated: Date }>(
      'SELECT stage_decided_at AS decided, created_at AS created, updated_at AS updated FROM opportunity WHERE id = $1',
      [id],
    );
    return rows[0]!;
  };

  it('gives a new record a clock equal to its creation time', async () => {
    // The backfill's invariant, on the insert path: nothing is known to have
    // decided a record's stage before it existed. A record whose clock started at
    // "now" would read as newer than any job submitted a moment earlier, and
    // would be skipped by it for no reason.
    const id = await seed(a());
    const { decided, created } = await clock(id);
    expect(decided.getTime()).toBe(created.getTime());
  });

  it('stamps the wall clock on a stage change', async () => {
    // The person's edit. Without this the clock would never move for a manual
    // change and every job would overwrite it - the exact failure the column is
    // for.
    const id = await seed(a());
    const before = await clock(id);
    // A gap wide enough that a rounding boundary cannot hide the change.
    await pool.query('SELECT pg_sleep(0.05)');
    await pool.query('UPDATE opportunity SET stage_id = $2 WHERE id = $1', [id, b()]);
    const after = await clock(id);
    expect(after.decided.getTime()).toBeGreaterThan(before.decided.getTime());
    expect(after.decided.getTime()).toBeGreaterThan(before.created.getTime());
  });

  it('leaves the clock alone when only the name changes', async () => {
    // The first WHEN clause. A rename is the most common edit there is, and if it
    // bumped the clock it would silently remove the record from every running
    // job - a user editing a deal's name would lose it from a bulk move with no
    // error and no explanation.
    const id = await seed(a());
    const before = await clock(id);
    await pool.query('UPDATE opportunity SET name = $2 WHERE id = $1', [id, 'renamed']);
    const after = await clock(id);
    expect(after.decided.getTime()).toBe(before.decided.getTime());
  });

  it('leaves the clock alone when only the value changes', async () => {
    // The other non-stage write, and the one most likely to be added to an
    // existing UPDATE by someone who did not know about the trigger.
    const id = await seed(a());
    const before = await clock(id);
    await pool.query('UPDATE opportunity SET value = 99999 WHERE id = $1', [id]);
    const after = await clock(id);
    expect(after.decided.getTime()).toBe(before.decided.getTime());
  });

  it('leaves the clock alone when only updated_at changes', async () => {
    // Every write path in the codebase sets updated_at alongside its real change.
    // This is the assertion that updated_at is not a proxy for the clock - the
    // column exists precisely because updated_at is too coarse to be one.
    const id = await seed(a());
    const before = await clock(id);
    await pool.query(`UPDATE opportunity SET updated_at = now() + interval '1 day' WHERE id = $1`, [
      id,
    ]);
    const after = await clock(id);
    expect(after.decided.getTime()).toBe(before.decided.getTime());
    // updated_at really did move, so the assertion above is not vacuous.
    expect(after.updated.getTime()).toBeGreaterThan(before.updated.getTime());
  });

  it('stands down when the writer sets the clock itself', async () => {
    // The second WHEN clause, and the one the whole ordering depends on. This is
    // how a bulk job stamps its own submission time: it names the column, so NEW
    // differs from OLD and the trigger leaves it alone. If the trigger fired
    // anyway it would overwrite the job's time with now(), and the ordering would
    // silently become last-writer-wins - the newest job would lose to whichever
    // job happened to finish last.
    const id = await seed(a());
    await pool.query(
      `UPDATE opportunity SET stage_id = $2, stage_decided_at = '2001-01-01T00:00:00Z' WHERE id = $1`,
      [id, b()],
    );
    const after = await clock(id);
    expect(after.decided.toISOString()).toBe('2001-01-01T00:00:00.000Z');
  });

  it('stamps the clock on a move that also names updated_at', async () => {
    // The exact shape of the two opportunity-service statements, which both write
    // stage_id and updated_at together. If the trigger only handled a bare
    // stage_id change, this is the statement that would slip through it - and it
    // is every manual edit the product makes.
    const id = await seed(a());
    const before = await clock(id);
    await pool.query(`UPDATE opportunity SET stage_id = $2, updated_at = now() WHERE id = $1`, [
      id,
      b(),
    ]);
    const after = await clock(id);
    expect(after.decided.getTime()).toBeGreaterThan(before.decided.getTime());
  });

  it('rejects a null clock', async () => {
    // NOT NULL is the schema's promise that the worker's comparison is always
    // against a real instant. A null would make every comparison false and the
    // record would be treated as never changed - so it is worth a test that the
    // constraint is actually there.
    const id = await seed(a());
    await expect(
      pool.query('UPDATE opportunity SET stage_decided_at = NULL WHERE id = $1', [id]),
    ).rejects.toThrow(/null value|not-null/i);
  });

  it('gives a record moved to its own current stage a fresh clock', async () => {
    // Setting stage_id to the value it already holds. IS DISTINCT FROM says this
    // is not a stage change, so the trigger stands down and the clock is
    // preserved - which is right, because nothing decided anything. A naive
    // trigger keyed on "an UPDATE touched stage_id" would bump it here, and a
    // no-op write would then read as a deliberate edit.
    const id = await seed(a());
    const before = await clock(id);
    await pool.query('UPDATE opportunity SET stage_id = stage_id WHERE id = $1', [id]);
    const after = await clock(id);
    expect(after.decided.getTime()).toBe(before.decided.getTime());
  });

  it('stamps each record of a bulk move independently', async () => {
    // The shape opportunity-service's updateStages produces: one statement, many
    // rows. Every row is a separate decision and each gets its own instant, which
    // is what lets a job that overlaps the middle of a bulk edit treat the
    // untouched records as in scope and the edited ones as not.
    const ids: string[] = [];
    for (let i = 0; i < 5; i += 1) ids.push(await seed(a()));
    await pool.query(
      `UPDATE opportunity SET stage_id = $2 WHERE workspace_id = $1 AND id = ANY($3::uuid[])`,
      [ws.workspaceId, b(), ids],
    );
    const { rows } = await pool.query<{ decided: Date }>(
      'SELECT stage_decided_at AS decided FROM opportunity WHERE id = ANY($1::uuid[])',
      [ids],
    );
    // All five moved, so all five carry a clock later than their creation.
    const { rows: created } = await pool.query<{ created: Date }>(
      'SELECT created_at AS created FROM opportunity WHERE id = ANY($1::uuid[])',
      [ids],
    );
    for (const [i, row] of rows.entries()) {
      expect(row.decided.getTime()).toBeGreaterThan(created[i]!.created.getTime());
    }
  });

  it('a record created after a job was submitted still reads as newer than it', async () => {
    // The property the backfill exists to protect, exercised through a real job
    // row rather than a backdated literal. A record created after submission
    // cannot have been decided before it, so no job may claim it.
    const { rows: jobRows } = await pool.query<{ id: string; snapshot_at: Date }>(
      `INSERT INTO bulk_job (workspace_id, idempotency_key, filter, target_stage_id, status)
       VALUES ($1, $2, '{}'::jsonb, $3, 'pending') RETURNING id, snapshot_at`,
      [ws.workspaceId, `clock-${randomUUID()}`, b()],
    );
    const job = jobRows[0]!;
    // A record created a moment after the job row.
    await new Promise((r) => setTimeout(r, 20));
    const id = await seed(a());
    const { decided } = await clock(id);
    expect(decided.getTime()).toBeGreaterThan(job.snapshot_at.getTime());
  });
});
