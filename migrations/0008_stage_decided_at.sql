-- 0008_stage_decided_at.sql
--
-- A logical clock on every opportunity, so a bulk job cannot overwrite a change
-- that was made after it was submitted, and cannot be overwritten by an older job
-- that happens to finish later.
--
-- The name is the point. This is NOT "when the stage last changed". For a write
-- made by a bulk job it holds that job's snapshot_at - when the job was
-- submitted, which may be minutes before the row is touched. For a write made by
-- a person it holds the wall-clock instant of their edit. It records whose
-- decision put the record in its current stage, so that a later-arriving worker
-- can tell whether that decision was newer or older than its own.
--
-- Real change times live in opportunity_transition.created_at, which is an
-- ordinary wall clock and is unaffected by any of this.
--
-- Why a column at all, when the worker already re-reads each record's live stage:
-- re-reading the stage answers "is this record still where the filter expected
-- it", which is not the same question. A person can move a record from stage A
-- to stage B while the filter names both A and B. The record is still in scope,
-- still has a permitted rule to the target, and the job would overwrite a
-- deliberate edit. Only a timestamp distinguishes that from a record nobody
-- touched.
--
-- The ordering rule the worker applies:
--
--   skip if record.stage_decided_at > job.snapshot_at
--
-- With a job writing its own snapshot_at rather than now(), this is an ordering
-- comparison on job submission time, so the newest job wins regardless of which
-- one finishes first. Writing now() instead would make it last-writer-wins, and
-- an older job still draining would beat a newer one that arrived after it.
--
-- Two properties fall out of that, and both are why the job writes its own
-- snapshot_at rather than a clock reading:
--
--   A job never skips its own write, because J.at > J.at is false. Without this,
--   a retry would see attempt 1's stamp as newer than its own submission and
--   classify all 1,000 records as skipped, moving nothing.
--
--   A person always wins, at either side of the submission. An edit before
--   submission is older than the job, so the job proceeds. An edit after is
--   newer, so the job defers.
--
-- The comparison is done in SQL, column against column, and no timestamp crosses
-- the wire. timestamptz carries microseconds; a JS Date carries milliseconds, and
-- this repository has no type parser override, so binding one would truncate both
-- sides of the comparison. The same trap is already documented in
-- shared/filter/snapshot-query.ts, where the keyset cursor is resolved by subquery
-- for exactly this reason.
--
-- Not a total order. Two jobs whose transactions start in the same microsecond
-- share a snapshot_at, neither defers, and the last writer wins. That is accepted
-- rather than fixed: the two jobs were submitted at the same instant, so "newer"
-- is genuinely undefined, and an arbitrary tiebreak between two simultaneous
-- intents is not a correctness problem. A bigserial sequence would remove the
-- case and is not worth a column.
--
-- ---------------------------------------------------------------------------
-- The backfill below is load-bearing and must not be simplified to DEFAULT now().
--
-- Adding the column with DEFAULT now() would stamp every existing row with the
-- migration time. Every job submitted before the migration has an earlier
-- snapshot_at, so all of its records would read as "decided after this job" and
-- be skipped - a job that reports zero processed and looks like a silent total
-- failure. created_at is the honest prior: nothing is known to have changed a
-- record before it existed.
--
-- The trigger sets the column for writers that do not set it themselves. The
-- second WHEN clause is what lets the worker opt out by naming the column: if
-- the UPDATE provides a value then NEW differs from OLD and the trigger stands
-- down, and if it does not then the column is untouched, NEW equals OLD, and the
-- trigger stamps the wall clock. The first clause is not optional either - without
-- it any UPDATE bumps the clock, and renaming a deal would silently remove it
-- from every in-flight job.

ALTER TABLE opportunity ADD COLUMN stage_decided_at timestamptz;

UPDATE opportunity SET stage_decided_at = created_at;

ALTER TABLE opportunity
    ALTER COLUMN stage_decided_at SET NOT NULL,
    ALTER COLUMN stage_decided_at SET DEFAULT now();

-- Stamps the wall clock on any stage change the writer did not timestamp itself.
-- A function rather than an inline trigger body because the WHEN clause is the
-- whole mechanism and is worth naming.
CREATE FUNCTION opportunity_stage_decided_at() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public
    AS $$
BEGIN
    NEW.stage_decided_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER opportunity_stage_decided_at_biu
    BEFORE UPDATE ON opportunity
    FOR EACH ROW
    WHEN (
        -- Only a real stage change. Without this, renaming a record or changing
        -- its owner would bump the clock and drop it from every in-flight job.
        OLD.stage_id IS DISTINCT FROM NEW.stage_id
        -- Only when the writer left the column alone. The worker sets it to its
        -- own snapshot_at, which makes NEW distinct from OLD and stands this down.
        AND NEW.stage_decided_at IS NOT DISTINCT FROM OLD.stage_decided_at
    )
    EXECUTE FUNCTION opportunity_stage_decided_at();
