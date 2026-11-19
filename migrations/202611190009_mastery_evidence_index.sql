-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY learning_mastery_recent_idx ON learning_task_outcomes (learner_id, graded_at DESC, submission_id)
    WHERE evidence_kind = 'independent';

-- +goose Down
DROP INDEX CONCURRENTLY learning_mastery_recent_idx;
