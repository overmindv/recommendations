-- +goose Up
CREATE TABLE study_activity (
    actor_id uuid NOT NULL,
    client_event_id uuid NOT NULL,
    payload_hash bytea NOT NULL,
    payload jsonb NOT NULL,
    accepted_seconds bigint NOT NULL CHECK (accepted_seconds BETWEEN 0 AND 30),
    trust text NOT NULL DEFAULT 'self_reported' CHECK (trust = 'self_reported'),
    received_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (actor_id, client_event_id)
);
CREATE INDEX study_activity_actor_received ON study_activity(actor_id, received_at DESC);
CREATE INDEX study_activity_expiry ON study_activity(expires_at);
CREATE TABLE study_activity_rate (
    actor_id uuid PRIMARY KEY,
    received_at timestamptz NOT NULL,
    window_start timestamptz NOT NULL,
    event_count integer NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX study_activity_rate_expiry ON study_activity_rate(expires_at);

-- +goose Down
DROP TABLE study_activity;
DROP TABLE study_activity_rate;
