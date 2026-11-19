-- +goose Up
CREATE TABLE study_plan_feedback (
    goal_id uuid NOT NULL,
    request_id uuid NOT NULL,
    plan_version bigint NOT NULL,
    action text NOT NULL CHECK (action IN ('too_easy', 'too_hard', 'not_relevant', 'skip', 'pause', 'resume', 'set_budget')),
    ordinal smallint,
    weekly_minutes integer,
    accepted_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (goal_id, request_id),
    FOREIGN KEY (goal_id, plan_version) REFERENCES study_plan_versions (goal_id, version) ON DELETE CASCADE,
    FOREIGN KEY (goal_id, plan_version, ordinal)
        REFERENCES study_plan_items (goal_id, plan_version, ordinal) ON DELETE CASCADE,
    CHECK (
        (action IN ('too_easy', 'too_hard', 'not_relevant', 'skip') AND ordinal IS NOT NULL AND weekly_minutes IS NULL)
        OR (action IN ('pause', 'resume') AND ordinal IS NULL AND weekly_minutes IS NULL)
        OR (action = 'set_budget' AND ordinal IS NULL AND weekly_minutes IS NOT NULL AND weekly_minutes BETWEEN 1 AND 10080)
    )
);

CREATE UNIQUE INDEX study_plan_feedback_skip_once
    ON study_plan_feedback (goal_id, plan_version, ordinal) WHERE action = 'skip';

CREATE INDEX study_plan_feedback_recent_idx ON study_plan_feedback (goal_id, accepted_at DESC);

CREATE INDEX study_plan_feedback_difficulty_idx ON study_plan_feedback (goal_id, accepted_at DESC, request_id)
    WHERE action IN ('too_easy', 'too_hard');

CREATE TABLE study_plan_feedback_state (
    goal_id uuid PRIMARY KEY REFERENCES learner_goals (id) ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    paused_at timestamptz,
    budget_from_plan bigint NOT NULL DEFAULT 0 CHECK (budget_from_plan >= 0),
    budget_goal_version bigint NOT NULL DEFAULT 0 CHECK (budget_goal_version >= 0),
    CHECK ((budget_from_plan = 0) = (budget_goal_version = 0))
);

CREATE TABLE study_plan_resource_cooldowns (
    goal_id uuid NOT NULL REFERENCES learner_goals (id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('theory', 'practice')),
    resource_id uuid NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (goal_id, kind, resource_id)
);

ALTER TABLE study_plan_rebuilds ADD COLUMN priority boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE study_plan_rebuilds DROP COLUMN priority;
DROP TABLE study_plan_resource_cooldowns;
DROP TABLE study_plan_feedback_state;
DROP TABLE study_plan_feedback;
