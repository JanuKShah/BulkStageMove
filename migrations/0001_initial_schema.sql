-- 0001_initial_schema.sql
--
-- Workspace-scoped core domain: workspace, user, stage, the rules governing
-- which stage may move to which, and opportunity.
--
-- Conventions:
--   * every table carries workspace_id and is never queried without it
--   * ids are uuid
--
-- Tenant isolation is enforced by the schema, not by application discipline.
-- Every reference to another tenant-owned row is a composite foreign key
-- carrying workspace_id, so a row can never point at another workspace's data
-- even if a caller passes a foreign id.
--
-- No secondary indexes here. The UNIQUE and PRIMARY KEY constraints below are
-- required for integrity and as FK targets.

CREATE TABLE workspace (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE app_user (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspace (id) ON DELETE CASCADE,
    name         text        NOT NULL,
    email        text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT app_user_workspace_email_uniq UNIQUE (workspace_id, email),
    -- FK target for the composite reference from opportunity
    CONSTRAINT app_user_id_workspace_uniq    UNIQUE (id, workspace_id)
);

-- The set of stages a workspace's pipeline is made of.
-- outcome is the single source of truth for an opportunity's status: a deal's
-- status is the outcome of the stage it sits in, so the two cannot disagree.
-- Permitted moves live in stage_transition_rule, which is the source of truth.
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

-- Which stage may move to which. A transition is a relation between two
-- stages, so it cannot live on stage itself. Absence of a row means the move
-- is not allowed - that is what makes a "non-transitionable" state detectable.
-- Both endpoints are pinned to the same workspace by composite FK.
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
    -- stage_id must belong to the same workspace as the opportunity
    CONSTRAINT opportunity_stage_fk
        FOREIGN KEY (stage_id, workspace_id) REFERENCES stage (id, workspace_id) ON DELETE RESTRICT,
    -- owner_id must belong to the same workspace as the opportunity
    CONSTRAINT opportunity_owner_fk
        FOREIGN KEY (owner_id, workspace_id) REFERENCES app_user (id, workspace_id) ON DELETE SET NULL
);
