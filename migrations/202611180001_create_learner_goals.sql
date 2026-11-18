-- +goose Up
CREATE TABLE learner_availability (
    user_id uuid PRIMARY KEY,
    weekly_minutes integer NOT NULL CHECK (weekly_minutes BETWEEN 0 AND 10080),
    timezone text NOT NULL CHECK (octet_length(timezone) BETWEEN 1 AND 64),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE learner_goals (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES learner_availability(user_id),
    title text NOT NULL CHECK (octet_length(title) BETWEEN 1 AND 256),
    template_version text NOT NULL DEFAULT '' CHECK (octet_length(template_version) <= 128),
    target_topics uuid[] NOT NULL CHECK (cardinality(target_topics) BETWEEN 1 AND 16 AND array_position(target_topics, NULL) IS NULL),
    deadline date,
    weekly_minutes integer NOT NULL CHECK (weekly_minutes BETWEEN 0 AND 10080),
    active boolean NOT NULL CHECK (NOT active OR weekly_minutes > 0),
    preferences jsonb NOT NULL CHECK (jsonb_typeof(preferences) = 'object' AND octet_length(preferences::text) <= 4096),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX learner_goals_owner_idx ON learner_goals (user_id, updated_at DESC, id);

CREATE TABLE learner_goal_requests (
    user_id uuid NOT NULL REFERENCES learner_availability(user_id),
    request_id uuid NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    response jsonb NOT NULL CHECK (octet_length(response::text) <= 8192),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (user_id, request_id)
);
CREATE INDEX learner_goal_requests_owner_expiry_idx ON learner_goal_requests (user_id, expires_at);
CREATE INDEX learner_goal_requests_expiry_idx ON learner_goal_requests (expires_at);

-- +goose Down
DROP TABLE learner_goal_requests;
DROP TABLE learner_goals;
DROP TABLE learner_availability;
