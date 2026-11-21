-- +goose Up
CREATE TABLE study_plan_rebuilds (
    goal_id uuid PRIMARY KEY REFERENCES learner_goals (id) ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    first_requested_at timestamptz NOT NULL,
    requested_at timestamptz NOT NULL CHECK (requested_at >= first_requested_at),
    available_at timestamptz NOT NULL CHECK (available_at >= first_requested_at),
    retry_at timestamptz,
    lease_token uuid,
    leased_until timestamptz,
    CHECK ((lease_token IS NULL) = (leased_until IS NULL))
);

CREATE INDEX study_plan_rebuilds_ready_idx ON study_plan_rebuilds (
    GREATEST(available_at, COALESCE(retry_at, '-infinity'::timestamptz), COALESCE(leased_until, '-infinity'::timestamptz)), goal_id
);

-- +goose Down
DROP TABLE study_plan_rebuilds;
