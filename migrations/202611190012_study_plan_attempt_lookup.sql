-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY learning_study_recent_idx
    ON learning_task_outcomes (learner_id, task_id, graded_at DESC);

-- +goose Down
DROP INDEX CONCURRENTLY learning_study_recent_idx;
