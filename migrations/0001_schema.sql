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

-- One row per opportunity the job must move: the snapshot, frozen at
-- submission. That is what makes resume trivial - there is no cursor to keep
-- consistent, because every item carries its own outcome.
--
-- from_stage_id is the stage the opportunity sat in when the job was submitted,
-- not when the worker reaches it. The worker compare-and-swaps on it, so a
-- record a user has moved by hand since submission is reported stale rather
-- than silently overwritten.
--
-- batch_no names the 1000-record page the item was submitted in, because the
-- page is the unit of dispatch and of retry. UNIQUE (job_id, opportunity_id)
-- means batches are disjoint, so two workers on one job cannot collide and a
-- filter that resolves an id twice cannot double-apply.
CREATE TABLE bulk_job_item (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         uuid        NOT NULL,
    workspace_id   uuid        NOT NULL,
    opportunity_id uuid        NOT NULL,
    from_stage_id  uuid        NOT NULL,
    batch_no       integer     NOT NULL,
    status         text        NOT NULL DEFAULT 'pending',
    attempts       integer     NOT NULL DEFAULT 0,
    error          text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz,
    CONSTRAINT bulk_job_item_status_valid
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    CONSTRAINT bulk_job_item_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT bulk_job_item_job_opportunity_uniq UNIQUE (job_id, opportunity_id),
    CONSTRAINT bulk_job_item_job_fk
        FOREIGN KEY (job_id, workspace_id)
        REFERENCES bulk_job (id, workspace_id) ON DELETE CASCADE,
    CONSTRAINT bulk_job_item_opportunity_fk
        FOREIGN KEY (opportunity_id, workspace_id)
        REFERENCES opportunity (id, workspace_id) ON DELETE CASCADE,
    -- DEFERRABLE, because from_stage_id is history rather than a live pointer.
    -- Deleting a workspace cascades to both stage and bulk_job_item, and an
    -- immediate check on either ordering fails: RESTRICT trips before the
    -- cascade reaches the job items, NO ACTION still trips mid-cascade.
    -- Deferred to commit, both are gone and the check passes. Tenant deletion
    -- depends on this, not just test teardown.
    CONSTRAINT bulk_job_item_from_stage_fk
        FOREIGN KEY (from_stage_id, workspace_id)
        REFERENCES stage (id, workspace_id) ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED
);

-- The dispatch outbox. A page of items and the intent to publish it commit
-- together, because publishing straight after the insert leaves a window where a
-- crash strands a page of pending items that no worker is ever told about, and
-- the job never leaves pending. The relay publishes and stamps published_at,
-- which makes delivery at-least-once; the worker's pending->running claim is
-- what makes processing effectively-once.
CREATE TABLE bulk_job_outbox (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL,
    job_id       uuid        NOT NULL,
    batch_no     integer     NOT NULL,
    item_count   integer     NOT NULL,
    attempts     integer     NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    CONSTRAINT bulk_job_outbox_job_batch_uniq UNIQUE (job_id, batch_no),
    CONSTRAINT bulk_job_outbox_item_count_positive CHECK (item_count > 0),
    CONSTRAINT bulk_job_outbox_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT bulk_job_outbox_job_fk
        FOREIGN KEY (job_id, workspace_id)
        REFERENCES bulk_job (id, workspace_id) ON DELETE CASCADE
);

-- A transition caused by a job points back at it. Manual moves leave it null,
-- which is how the two are told apart. SET NULL so the audit trail outlives the
-- job row.
ALTER TABLE opportunity_transition
    ADD COLUMN job_id uuid REFERENCES bulk_job (id) ON DELETE SET NULL;

-- --------------------------------------------------------------------- indexes

-- The worker's claim is WHERE job_id = $1 AND batch_no = $2 AND status =
-- 'pending'. At 50,000 items per job with several jobs in flight that is a
-- sequential scan per batch without this.
CREATE INDEX bulk_job_item_pending_idx
    ON bulk_job_item (job_id, batch_no)
    WHERE status = 'pending';

-- The relay's only query: unsent rows, oldest first, so batches publish in the
-- order they were submitted.
CREATE INDEX bulk_job_outbox_unpublished_idx
    ON bulk_job_outbox (created_at)
    WHERE published_at IS NULL;
