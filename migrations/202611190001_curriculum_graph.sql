-- +goose Up
CREATE TABLE catalog_projection_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    revision BIGINT NOT NULL DEFAULT 0,
    active_generation BIGINT NOT NULL DEFAULT 0,
    applied_through BIGINT NOT NULL DEFAULT 0,
    replay_watermark BIGINT,
    pending_generation BIGINT NOT NULL DEFAULT 0,
    pending_watermark BIGINT NOT NULL DEFAULT 0,
    pending_cursor TEXT NOT NULL DEFAULT '',
    pending_rows BIGINT NOT NULL DEFAULT 0,
    rebuild BOOLEAN NOT NULL DEFAULT true,
    rejected_watermark BIGINT NOT NULL DEFAULT -1
);
INSERT INTO catalog_projection_state DEFAULT VALUES;

CREATE TABLE catalog_projected_topics (
    generation BIGINT NOT NULL CHECK (generation > 0),
    topic_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    tombstone BOOLEAN NOT NULL,
    metadata JSONB,
    PRIMARY KEY (generation, topic_id),
    CHECK (tombstone = (metadata IS NULL))
);

CREATE TABLE catalog_projected_prerequisites (
    generation BIGINT NOT NULL CHECK (generation > 0),
    topic_id UUID NOT NULL,
    prerequisite_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    tombstone BOOLEAN NOT NULL,
    PRIMARY KEY (generation, topic_id, prerequisite_id),
    CHECK (topic_id <> prerequisite_id)
);

CREATE TABLE catalog_projection_inbox (
    event_id UUID PRIMARY KEY,
    revision BIGINT NOT NULL UNIQUE CHECK (revision > 0),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32)
);

CREATE TABLE catalog_projection_quarantine (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase TEXT NOT NULL CHECK (phase IN ('snapshot','replay')),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    event_id UUID,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE catalog_projection_retired (
    generation BIGINT PRIMARY KEY CHECK (generation > 0)
);

-- +goose Down
DROP TABLE catalog_projection_retired;
DROP TABLE catalog_projection_quarantine;
DROP TABLE catalog_projection_inbox;
DROP TABLE catalog_projected_prerequisites;
DROP TABLE catalog_projected_topics;
DROP TABLE catalog_projection_state;
