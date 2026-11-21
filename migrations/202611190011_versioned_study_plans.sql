-- +goose Up
CREATE TABLE study_plan_versions (
    goal_id uuid NOT NULL REFERENCES learner_goals (id) ON DELETE CASCADE,
    version bigint NOT NULL CHECK (version > 0),
    goal_version bigint NOT NULL CHECK (goal_version > 0),
    availability_version bigint NOT NULL CHECK (availability_version > 0),
    reserved_minutes integer NOT NULL CHECK (reserved_minutes BETWEEN 0 AND 10080),
    target_topics uuid[] NOT NULL CHECK (
        cardinality(target_topics) BETWEEN 1 AND 16 AND array_position(target_topics, NULL) IS NULL
    ),
    planner_version text NOT NULL CHECK (octet_length(planner_version) BETWEEN 1 AND 64),
    ranking_algorithm text NOT NULL CHECK (octet_length(ranking_algorithm) BETWEEN 1 AND 64),
    policy_version text NOT NULL CHECK (octet_length(policy_version) BETWEEN 1 AND 64),
    ranking_policy jsonb NOT NULL CHECK (
        jsonb_typeof(ranking_policy) = 'object' AND octet_length(ranking_policy::text) <= 4096
        AND (ranking_policy->>'policy_version') IS NOT DISTINCT FROM policy_version
    ),
    evidence_watermark bytea NOT NULL CHECK (octet_length(evidence_watermark) = 32),
    evidence_policy jsonb NOT NULL CHECK (
        jsonb_typeof(evidence_policy) = 'object' AND octet_length(evidence_policy::text) <= 1024
    ),
    calendar jsonb NOT NULL CHECK (
        jsonb_typeof(calendar) = 'object' AND octet_length(calendar::text) <= 131072
    ),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (goal_id, version)
);

CREATE TABLE study_plan_heads (
    goal_id uuid PRIMARY KEY REFERENCES learner_goals (id) ON DELETE CASCADE,
    version bigint NOT NULL,
    FOREIGN KEY (goal_id, version) REFERENCES study_plan_versions (goal_id, version) ON DELETE CASCADE
);

CREATE TABLE study_plan_items (
    goal_id uuid NOT NULL,
    plan_version bigint NOT NULL,
    ordinal smallint NOT NULL CHECK (ordinal BETWEEN 1 AND 128),
    week_number smallint NOT NULL CHECK (week_number BETWEEN 1 AND 26),
    topic_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    resource_version_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('theory', 'practice')),
    stage text NOT NULL CHECK (
        (kind = 'theory' AND stage = 'theory')
        OR (kind = 'practice' AND stage IN ('practice', 'independent_check'))
    ),
    estimated_minutes smallint NOT NULL CHECK (estimated_minutes BETWEEN 1 AND 10080),
    score integer NOT NULL CHECK (score BETWEEN 0 AND 50000000),
    score_components jsonb NOT NULL CHECK (
        jsonb_typeof(score_components) = 'object' AND octet_length(score_components::text) <= 1024
    ),
    requires_checks uuid[] NOT NULL CHECK (
        cardinality(requires_checks) <= 64 AND array_position(requires_checks, NULL) IS NULL
    ),
    PRIMARY KEY (goal_id, plan_version, ordinal),
    UNIQUE (goal_id, plan_version, ordinal, stage),
    UNIQUE (goal_id, plan_version, kind, resource_id),
    FOREIGN KEY (goal_id, plan_version) REFERENCES study_plan_versions (goal_id, version) ON DELETE CASCADE
);

CREATE TABLE study_plan_activity (
    goal_id uuid NOT NULL,
    plan_version bigint NOT NULL,
    ordinal smallint NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (goal_id, plan_version, ordinal),
    FOREIGN KEY (goal_id, plan_version, ordinal)
        REFERENCES study_plan_items (goal_id, plan_version, ordinal) ON DELETE CASCADE
);

CREATE TABLE study_plan_assessments (
    goal_id uuid NOT NULL,
    plan_version bigint NOT NULL,
    ordinal smallint NOT NULL,
    stage text NOT NULL DEFAULT 'independent_check' CHECK (stage = 'independent_check'),
    submission_id uuid NOT NULL,
    event_id uuid NOT NULL,
    correction_version bigint NOT NULL CHECK (correction_version > 0),
    verdict text NOT NULL CHECK (verdict IN ('accepted', 'wrong_answer', 'compilation_error',
        'runtime_error', 'time_limit_exceeded', 'memory_limit_exceeded', 'output_limit_exceeded')),
    graded_at timestamptz NOT NULL,
    PRIMARY KEY (goal_id, plan_version, ordinal),
    FOREIGN KEY (goal_id, plan_version, ordinal, stage)
        REFERENCES study_plan_items (goal_id, plan_version, ordinal, stage) ON DELETE CASCADE
);

CREATE TABLE study_plan_requests (
    goal_id uuid NOT NULL,
    request_id uuid NOT NULL,
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    plan_version bigint NOT NULL,
    PRIMARY KEY (goal_id, request_id),
    FOREIGN KEY (goal_id, plan_version) REFERENCES study_plan_versions (goal_id, version) ON DELETE CASCADE
);

CREATE INDEX study_plan_requests_version_idx ON study_plan_requests (goal_id, plan_version);

-- +goose Down
DROP TABLE study_plan_requests;
DROP TABLE study_plan_assessments;
DROP TABLE study_plan_activity;
DROP TABLE study_plan_items;
DROP TABLE study_plan_heads;
DROP TABLE study_plan_versions;
