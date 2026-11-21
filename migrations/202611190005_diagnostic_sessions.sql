-- +goose Up
CREATE TABLE diagnostic_sessions (
    id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    goal_id uuid PRIMARY KEY REFERENCES learner_goals (id) ON DELETE CASCADE,
    goal_version bigint NOT NULL CHECK (goal_version > 0),
    status text NOT NULL CHECK (status IN ('ready', 'no_tasks', 'skipped')),
    policy_version text NOT NULL CHECK (octet_length(policy_version) BETWEEN 1 AND 64),
    max_tasks smallint NOT NULL CHECK (max_tasks BETWEEN 1 AND 4),
    budget_minutes smallint NOT NULL CHECK (budget_minutes BETWEEN 0 AND 20),
    estimated_minutes smallint NOT NULL CHECK (estimated_minutes BETWEEN 0 AND budget_minutes),
    tasks jsonb NOT NULL CHECK (jsonb_typeof(tasks) = 'array' AND octet_length(tasks::text) <= 2048),
    started_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    skipped_at timestamptz,
    CHECK (jsonb_array_length(tasks) <= max_tasks),
    CHECK (status <> 'ready' OR jsonb_array_length(tasks) > 0),
    CHECK (status <> 'no_tasks' OR jsonb_array_length(tasks) = 0),
    CHECK ((estimated_minutes = 0) = (jsonb_array_length(tasks) = 0)),
    CHECK ((status = 'skipped') = (skipped_at IS NOT NULL)),
    CHECK (status = 'skipped' OR budget_minutes > 0),
    CHECK (expires_at = started_at + budget_minutes * interval '1 minute')
);

-- +goose Down
DROP TABLE diagnostic_sessions;
