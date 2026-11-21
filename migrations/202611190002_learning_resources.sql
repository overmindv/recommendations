-- +goose Up
CREATE TABLE resource_projection_checkpoints (
    origin text PRIMARY KEY CHECK (origin IN ('tasks', 'content')),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    active_generation bigint NOT NULL DEFAULT 0 CHECK (active_generation >= 0),
    progress jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object')
);

INSERT INTO resource_projection_checkpoints (origin) VALUES ('tasks'), ('content');

CREATE TABLE resource_projection_items (
    origin text NOT NULL REFERENCES resource_projection_checkpoints (origin),
    generation bigint NOT NULL CHECK (generation > 0),
    resource_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    revision_id uuid,
    metadata jsonb,
    PRIMARY KEY (origin, generation, resource_id),
    CHECK ((revision_id IS NULL) = (metadata IS NULL)),
    CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object')
);

CREATE TABLE resource_projection_topics (
    origin text NOT NULL,
    generation bigint NOT NULL,
    resource_id uuid NOT NULL,
    topic_id uuid NOT NULL,
    weight integer NOT NULL CHECK (weight BETWEEN 1 AND 10000),
    PRIMARY KEY (origin, generation, resource_id, topic_id),
    FOREIGN KEY (origin, generation, resource_id)
        REFERENCES resource_projection_items (origin, generation, resource_id) ON DELETE CASCADE
);

CREATE INDEX resource_projection_topics_lookup
    ON resource_projection_topics (origin, generation, topic_id, resource_id) INCLUDE (weight);

CREATE TABLE resource_projection_inbox (
    origin text NOT NULL REFERENCES resource_projection_checkpoints (origin),
    generation bigint NOT NULL CHECK (generation > 0),
    event_id uuid NOT NULL,
    resource_version bigint NOT NULL CHECK (resource_version > 0),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    PRIMARY KEY (origin, generation, event_id),
    UNIQUE (origin, generation, resource_version)
);

CREATE TABLE resource_projection_retired (
    origin text NOT NULL REFERENCES resource_projection_checkpoints (origin),
    generation bigint NOT NULL CHECK (generation > 0),
    PRIMARY KEY (origin, generation)
);

-- +goose Down
DROP TABLE resource_projection_retired;
DROP TABLE resource_projection_inbox;
DROP TABLE resource_projection_topics;
DROP TABLE resource_projection_items;
DROP TABLE resource_projection_checkpoints;
