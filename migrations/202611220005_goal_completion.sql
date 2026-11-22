-- +goose Up
CREATE TABLE goal_completion_checks (
    learner_id uuid PRIMARY KEY,
    token uuid NOT NULL DEFAULT gen_random_uuid(),
    ready_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE INDEX goal_completion_checks_ready_idx ON goal_completion_checks (ready_at, learner_id);

CREATE TABLE goal_completion_state (
    goal_id uuid PRIMARY KEY,
    learner_id uuid NOT NULL,
    completion_version bigint NOT NULL CHECK (completion_version > 0),
    verified boolean NOT NULL,
    goal_version bigint NOT NULL CHECK (goal_version > 0),
    plan_version bigint NOT NULL CHECK (plan_version > 0),
    topics uuid[] NOT NULL CHECK (cardinality(topics) BETWEEN 1 AND 16 AND array_position(topics, NULL) IS NULL),
    changed_at timestamptz NOT NULL
);

CREATE INDEX goal_completion_verified_idx ON goal_completion_state (learner_id, goal_id) WHERE verified;

CREATE TABLE goal_completion_outbox (
    event_id uuid PRIMARY KEY,
    goal_id uuid NOT NULL,
    completion_version bigint NOT NULL CHECK (completion_version > 0),
    payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 8192),
    ready_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    lease_token uuid,
    leased_until timestamptz,
    published_at timestamptz,
    UNIQUE (goal_id, completion_version),
    CHECK ((lease_token IS NULL) = (leased_until IS NULL)),
    CHECK (published_at IS NULL OR lease_token IS NULL)
);

CREATE INDEX goal_completion_outbox_pending_idx ON goal_completion_outbox (goal_id, completion_version)
    WHERE published_at IS NULL;
CREATE INDEX goal_completion_outbox_ready_idx ON goal_completion_outbox (
    GREATEST(ready_at, COALESCE(leased_until, '-infinity'::timestamptz)), goal_id
) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE goal_completion_outbox;
DROP TABLE goal_completion_state;
DROP TABLE goal_completion_checks;
