-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY learning_study_accepted_idx
    ON learning_task_outcomes (learner_id, task_id, graded_at) WHERE verdict = 'accepted';

-- +goose Down
DROP INDEX CONCURRENTLY learning_study_accepted_idx;
