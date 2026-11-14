-- +goose Up
ALTER TABLE practice_goals ADD COLUMN session_count integer NOT NULL DEFAULT 1
    CHECK (session_count BETWEEN 1 AND 8);

-- +goose Down
ALTER TABLE practice_goals DROP COLUMN session_count;
