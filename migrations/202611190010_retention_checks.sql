-- +goose Up
CREATE TABLE retention_checks (
    goal_id uuid NOT NULL REFERENCES learner_goals (id) ON DELETE CASCADE,
    topic_id uuid NOT NULL,
    goal_version bigint NOT NULL CHECK (goal_version > 0),
    due_at timestamptz NOT NULL,
    assigned_at timestamptz NOT NULL CHECK (assigned_at >= due_at),
    expires_at timestamptz NOT NULL CHECK (expires_at > assigned_at),
    task_id uuid NOT NULL,
    task_version_id uuid NOT NULL,
    estimated_minutes smallint NOT NULL CHECK (estimated_minutes BETWEEN 1 AND 20),
    PRIMARY KEY (goal_id, topic_id)
);

-- +goose Down
DROP TABLE retention_checks;
