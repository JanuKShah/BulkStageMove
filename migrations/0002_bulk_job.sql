-- 0002_bulk_job.sql
--
-- The bulk stage move: a submitted job, the batches it was split into, and a
-- link from every transition the job caused back to the job that caused it.
--
-- Snapshot semantics: the matching opportunities are resolved once at submission
-- and stored on the batch, so the job moves exactly the set the user saw. The
-- alternative - re-evaluating the filter as work progresses - was not chosen
-- because it makes "what did this job actually change?" unanswerable
-- afterwards, which is what the progress endpoint exists to answer.
--
-- The cost is recorded honestly: an opportunity created after submission is
-- never picked up, even if it would have matched the filter. A workspace
-- re-running the same filter is how that case gets handled.
--
-- Counts on bulk_job are denormalised for cheap progress reads. They are
-- written in the same transaction as the item status they summarise, so they
-- cannot drift from bulk_job_item. bulk_job_item is the authoritative record.
--
-- There is no 'cancelled' state, by decision. Once submitted, a job runs to
-- completion; the only terminal states are 'completed' and 'failed'. That makes
-- progress monotonic and keeps half-applied jobs to exactly one cause - a
-- process dying - which is the same state resume already has to handle. The
-- cost is that a caller who submits a bad filter over 50,000 records waits it
-- out; the brief scopes out any UI, so there is no cancel affordance to regret
-- in the first place.

CREATE TABLE bulk_job (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    -- Supplied by the caller, one value per logical submission. The UI generates
    -- it when the user clicks submit and reuses it for a retry of that same
    -- click, so a retry resolves to the existing job instead of starting a
    -- second one.
    --
    -- Deliberately NOT a hash of the request contents. A hash of
    -- (workspace, target stage, filter) can only tell "identical request", and a
    -- deliberate re-run of the same filter is an identical request - it would be
    -- rejected as a replay. With a client key, a retry and a re-run are
    -- distinguishable.
    idempotency_key  text        NOT NULL,
    -- Kept so the job remains explainable after the fact, and so a replay of
    -- the same key can be checked for a mismatched filter or target stage.
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
    -- One job per (workspace, idempotency key). Several unfinished jobs may run
    -- in the same workspace as long as they carry different keys; the key is
    -- what makes a client retry safe, not a lock on the workspace.
    CONSTRAINT bulk_job_workspace_idempotency_uniq UNIQUE (workspace_id, idempotency_key),
    -- FK target for the composite reference from bulk_job_item
    CONSTRAINT bulk_job_id_workspace_uniq UNIQUE (id, workspace_id),
    CONSTRAINT bulk_job_target_stage_fk
        FOREIGN KEY (target_stage_id, workspace_id)
        REFERENCES stage (id, workspace_id) ON DELETE RESTRICT
);

-- One row per opportunity the job has to move. This is the snapshot, and it is
-- what makes resume trivial: there is no cursor to keep consistent, because
-- every item carries its own outcome. A killed worker leaves items in
-- 'pending' or 'running', and the next pass picks up exactly those.
--
-- Batching is therefore a runtime concern - the worker takes a page of pending
-- items - rather than something stored. That is why there is no batch column
-- and no batch id.
--
-- The same opportunity cannot appear twice in one job, so a filter that
-- resolves an id twice cannot double-apply.
CREATE TABLE bulk_job_item (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         uuid        NOT NULL,
    workspace_id   uuid        NOT NULL,
    opportunity_id uuid        NOT NULL,
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
    -- the job and the opportunity must both belong to the item's workspace
    CONSTRAINT bulk_job_item_job_fk
        FOREIGN KEY (job_id, workspace_id)
        REFERENCES bulk_job (id, workspace_id) ON DELETE CASCADE,
    CONSTRAINT bulk_job_item_opportunity_fk
        FOREIGN KEY (opportunity_id, workspace_id)
        REFERENCES opportunity (id, workspace_id) ON DELETE CASCADE
);

-- A transition caused by a bulk job points back at that job. Manual moves leave
-- it null, which is how the two are told apart. SET NULL because job rows are
-- retained for 60 days and the audit trail must outlive them.
ALTER TABLE opportunity_transition
    ADD COLUMN job_id uuid REFERENCES bulk_job (id) ON DELETE SET NULL;
