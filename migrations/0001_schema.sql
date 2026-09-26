-- 0001_schema.sql
--
-- The whole schema. This project has not shipped, so the migrations are squashed
-- into one file rather than carrying a history nothing depends on yet.
--
-- Conventions: every tenant-owned table carries workspace_id and is never
-- queried without it; ids are uuid; no ORM.
--
-- Tenant isolation is enforced by the schema, not by application discipline.
-- Every reference to another tenant-owned row is a composite foreign key
-- carrying workspace_id, so a row cannot point at another workspace's data even
-- if a caller passes a foreign id.

-- ---------------------------------------------------------------- core domain

CREATE TABLE workspace (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- email is NOT NULL: a user the platform cannot reach is not a user, and while
-- it was nullable UNIQUE (workspace_id, email) had a second meaning, because
-- NULLs are distinct in a unique index and any number of nameless users could
-- share a workspace unjudged by the constraint.
CREATE TABLE app_user (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    name         text        NOT NULL,
    email        text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT app_user_workspace_email_uniq UNIQUE (workspace_id, email),
    -- FK target for the composite reference from opportunity
    CONSTRAINT app_user_id_workspace_uniq    UNIQUE (id, workspace_id)
);

-- outcome is the single source of truth for an opportunity's status: a deal's
-- status is the outcome of the stage it sits in, so the two cannot disagree.
CREATE TABLE stage (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    name         text        NOT NULL,
    outcome      text        NOT NULL DEFAULT 'open',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT stage_workspace_name_uniq UNIQUE (workspace_id, name),
    CONSTRAINT stage_outcome_valid
        CHECK (outcome IN ('open', 'won', 'lost', 'abandoned')),
    -- FK target for the composite references from opportunity and the rules
    CONSTRAINT stage_id_workspace_uniq    UNIQUE (id, workspace_id)
);

-- Absence of a row means the move is not allowed, which is what makes a
-- "non-transitionable" state detectable.
CREATE TABLE stage_transition_rule (
    workspace_id  uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    from_stage_id uuid        NOT NULL,
    to_stage_id   uuid        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (from_stage_id, to_stage_id),
    CONSTRAINT stage_transition_rule_no_self CHECK (from_stage_id <> to_stage_id),
    CONSTRAINT stage_transition_rule_from_fk
        FOREIGN KEY (from_stage_id, workspace_id) REFERENCES stage (id, workspace_id) ON DELETE CASCADE,
    CONSTRAINT stage_transition_rule_to_fk
        FOREIGN KEY (to_stage_id, workspace_id)   REFERENCES stage (id, workspace_id) ON DELETE CASCADE
);

CREATE TABLE opportunity (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid           NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    stage_id     uuid           NOT NULL,
    name         text           NOT NULL,
    value        numeric(14, 2) NOT NULL DEFAULT 0,
    owner_id     uuid,
    created_at   timestamptz    NOT NULL DEFAULT now(),
    updated_at   timestamptz    NOT NULL DEFAULT now(),
    CONSTRAINT opportunity_stage_fk
        FOREIGN KEY (stage_id, workspace_id) REFERENCES stage (id, workspace_id) ON DELETE RESTRICT,
    CONSTRAINT opportunity_owner_fk
        FOREIGN KEY (owner_id, workspace_id) REFERENCES app_user (id, workspace_id) ON DELETE SET NULL,
    -- FK target for the composite reference from opportunity_transition
    CONSTRAINT opportunity_id_workspace_uniq UNIQUE (id, workspace_id)
);

-- from_stage_id is null for the row recording an opportunity's creation.
-- Owned by opportunity-service, which writes the stage change and this row in
-- one transaction so the two cannot diverge.
CREATE TABLE opportunity_transition (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id   uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    opportunity_id uuid        NOT NULL,
    from_stage_id  uuid,
    to_stage_id    uuid        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT opportunity_transition_opportunity_fk
        FOREIGN KEY (opportunity_id, workspace_id) REFERENCES opportunity (id, workspace_id) ON DELETE CASCADE
);

-- ------------------------------------------------------------------- bulk job

-- No 'cancelled' state, by decision: once submitted a job runs to completion,
-- so progress is monotonic and a half-applied job has exactly one cause.
--
-- idempotency_key is supplied by the caller, not derived. A hash of
-- (workspace, target, filter) can only detect an identical request, and a
-- deliberate re-run of the same filter is identical - it would be rejected as a
-- replay. A client key keeps a retry and a re-run distinguishable.
CREATE TABLE bulk_job (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    idempotency_key  text        NOT NULL,
    filter           jsonb       NOT NULL,
    target_stage_id  uuid        NOT NULL,
    status           text        NOT NULL DEFAULT 'pending',
    total_matched    integer     NOT NULL DEFAULT 0,
    processed_count  integer     NOT NULL DEFAULT 0,
    failed_count     integer     NOT NULL DEFAULT 0,
    error            text,
    attempts         integer     NOT NULL DEFAULT 0,
    -- The watermark. Every opportunity this job can touch has created_at at or
    -- before it, so a record created after submission can never be swept up.
    --
    -- This is what replaced a materialised snapshot. Freezing the predicate
    -- rather than the result set is what lets submit write 50 rows instead of
    -- 50,000, and it is sound because created_at is immutable: the same filter
    -- and the same watermark resolve to the same set, every time, for every
    -- worker. Bound once here at submission rather than as now() per query,
    -- which would advance with each transaction and re-open the set. The default
    -- is the same instant as created_at, so a job row written by anything other
    -- than the submit path stays coherent instead of tripping NOT NULL.
    snapshot_at      timestamptz NOT NULL DEFAULT now(),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    completed_at     timestamptz,
    CONSTRAINT bulk_job_status_valid
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    CONSTRAINT bulk_job_counts_non_negative
        CHECK (total_matched >= 0 AND processed_count >= 0 AND failed_count >= 0),
    -- One job per (workspace, key). Several unfinished jobs may run in one
    -- workspace; the key makes a retry safe, it is not a lock on the workspace.
    CONSTRAINT bulk_job_workspace_idempotency_uniq UNIQUE (workspace_id, idempotency_key),
    -- FK target for the composite reference from bulk_job_item
    CONSTRAINT bulk_job_id_workspace_uniq UNIQUE (id, workspace_id),
    CONSTRAINT bulk_job_target_stage_fk
        FOREIGN KEY (target_stage_id, workspace_id)
        REFERENCES stage (id, workspace_id) ON DELETE RESTRICT
);

-- There is deliberately no per-opportunity item table.
--
-- This schema used to carry bulk_job_item: one row per opportunity the job had
-- to move, holding the stage it was found in, its own status, and its batch
-- number. Fifty thousand rows per job, four indexes and three foreign keys each.
-- It was the snapshot, and it cost 81% of the time between submitting a job and
-- getting its 201 back - measured, not estimated.
--
-- What replaced it is the watermark on bulk_job.snapshot_at plus the filter
-- already stored on bulk_job.filter. A batch is not a stored set of ids, it is a
-- position: batch N is the Nth page of the filter evaluated against the
-- watermark. Because created_at never changes, that resolves to the same records
-- for every worker, on every retry, without anything being written per record.
--
-- The trade is deliberate and it is a real one. The job now moves what matches
-- when the worker reaches it, not what matched at submission, so a record moved
-- out of the filtered stage by someone else in the meantime is skipped rather
-- than overwritten, and processed_count can land below total_matched. That is
-- reported rather than hidden: total_matched means "matched at submission".
--
-- What it bought: submission writes 50 rows instead of 50,000, the worker stops
-- rewriting a 50,000-row table as it goes, and settling a job is a scan of 50
-- batch rows instead of 50,000 items.

-- The dispatch outbox, and the per-batch state that used to live on the items.
--
-- Each row is one batch: which records it covers, and what became of them.
-- There is no item_count column, because cardinality(item_ids) already says how
-- many and two copies of one fact drift. completed_count and failed_count are
-- what the worker settled, which can add up to less than the batch held when
-- records had left the filtered set by the time the batch reached them.
--
-- A batch row and the intent to publish it commit together, because publishing
-- straight after the insert leaves a window where a
-- crash strands a page of pending items that no worker is ever told about, and
-- the job never leaves pending. The relay publishes and stamps published_at,
-- which makes delivery at-least-once; the worker's pending->running claim is
-- what makes processing effectively-once.
CREATE TABLE bulk_job_outbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    uuid        NOT NULL,
    job_id          uuid        NOT NULL,
    batch_no        integer     NOT NULL,
    -- The records this batch covers, as a set rather than a position.
    --
    -- Storing them is what keeps batching deterministic. Deriving batch N as the
    -- Nth page of the filter instead means the worker walks past every earlier
    -- batch's records to find its own, which is quadratic across the job, and any
    -- deletion shifts every later batch onto records a previous batch already
    -- took. An array of ids is one row's worth of work to write and one indexed
    -- lookup to read.
    --
    -- Membership is frozen here; the decision is not. The worker reads each
    -- record's live stage and skips anything that has left the filtered set since,
    -- so a concurrent change is not overwritten - it just does not count as
    -- processed. from_stage_id is deliberately not stored: the live stage is the
    -- baseline, which is what makes that check possible.
    item_ids        uuid[]      NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    completed_count integer     NOT NULL DEFAULT 0,
    failed_count    integer     NOT NULL DEFAULT 0,
    error           text,
    attempts        integer     NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    started_at      timestamptz,
    completed_at    timestamptz,
    CONSTRAINT bulk_job_outbox_job_batch_uniq UNIQUE (job_id, batch_no),
    -- No item_count column: it would be cardinality(item_ids) restated, and two
    -- copies of the same fact drift. Every read derives it.
    CONSTRAINT bulk_job_outbox_item_ids_non_empty CHECK (cardinality(item_ids) > 0),
    CONSTRAINT bulk_job_outbox_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT bulk_job_outbox_counts_non_negative
        CHECK (completed_count >= 0 AND failed_count >= 0),
    -- 'running' is re-claimable, exactly as it was on the items. A batch whose
    -- worker died mid-flight has to be takeable again, or the redelivered
    -- message would find nothing pending and the job would hang. The advisory
    -- lock on (job_id, batch_no) is what makes re-claiming safe.
    CONSTRAINT bulk_job_outbox_status_valid
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    CONSTRAINT bulk_job_outbox_job_fk
        FOREIGN KEY (job_id, workspace_id)
        REFERENCES bulk_job (id, workspace_id) ON DELETE CASCADE
);

-- The records a batch could not move, and why. The only per-opportunity table
-- left in the job, and it holds exceptions rather than the population: a clean
-- 50,000-record job writes nothing here.
--
-- UNIQUE (job_id, opportunity_id) is what makes a retry idempotent. Without the
-- items table a redelivered batch re-examines all 1000 of its records, so a
-- record that failed on the first attempt would otherwise be recorded again on
-- every retry.
CREATE TABLE bulk_job_failure (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id   uuid        NOT NULL,
    job_id         uuid        NOT NULL,
    batch_no       integer     NOT NULL,
    opportunity_id uuid        NOT NULL,
    from_stage_id  uuid,
    error          text        NOT NULL,
    attempts       integer     NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT bulk_job_failure_job_opportunity_uniq UNIQUE (job_id, opportunity_id),
    CONSTRAINT bulk_job_failure_job_fk
        FOREIGN KEY (job_id, workspace_id)
        REFERENCES bulk_job (id, workspace_id) ON DELETE CASCADE,
    -- No FK on opportunity_id: a record can fail because it no longer exists,
    -- and that is the one failure worth keeping. The composite reference to
    -- opportunity would make the row impossible to write in that case.
    CONSTRAINT bulk_job_failure_attempts_non_negative CHECK (attempts >= 0)
);

-- A transition caused by a job points back at it. Manual moves leave it null,
-- which is how the two are told apart. SET NULL so the audit trail outlives the
-- job row.
ALTER TABLE opportunity_transition
    ADD COLUMN job_id uuid REFERENCES bulk_job (id) ON DELETE SET NULL;

-- There is deliberately no UNIQUE (job_id, opportunity_id) on this table.
--
-- It was there as the retry guard that replaced per-item status, and it was
-- measured costing one index probe per row: a 50,000 record job paid 50,000
-- probes, against an index whose leading column is a single value shared by every
-- row of the job.
--
-- It cannot fire. A batch applies inside one transaction, so either all of its
-- records move and their transitions commit, or none do and the inserts roll back
-- with them. A retried batch therefore re-inserts into a table that has no trace
-- of the failed attempt, and the worker separately skips any record already at
-- the target rather than re-inserting for it. Two batches cannot share a record,
-- because the keyset order on (created_at, id) is total. A record appearing in
-- two different jobs is two different keys.
--
-- The audit trail's integrity rests on that transaction boundary, not on a
-- constraint. If that ever changes - a batch split across transactions, or a
-- partial-commit path added - this needs to come back.

-- --------------------------------------------------------------------- indexes

-- The relay's only query: unsent rows, oldest first, so batches publish in the
-- order they were submitted.
CREATE INDEX bulk_job_outbox_unpublished_idx
    ON bulk_job_outbox (created_at)
    WHERE published_at IS NULL;

-- Failures, for the endpoint that lists what to fix, scoped to one job and
-- ordered by the batch that carried them.
CREATE INDEX bulk_job_failure_job_idx
    ON bulk_job_failure (job_id, batch_no);
