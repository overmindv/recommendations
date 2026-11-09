-- +goose Up
CREATE TABLE practice_goals (
    user_id UUID PRIMARY KEY,
    topic_ids JSONB NOT NULL,
    minutes INTEGER NOT NULL CHECK (minutes BETWEEN 10 AND 240),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT practice_goals_topics_array CHECK (jsonb_typeof(topic_ids) = 'array')
);

-- +goose Down
DROP TABLE practice_goals;
