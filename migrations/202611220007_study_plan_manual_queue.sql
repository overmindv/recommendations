-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY study_plan_rebuilds_manual_ready_idx ON study_plan_rebuilds (
    GREATEST(available_at, COALESCE(retry_at, '-infinity'::timestamptz), COALESCE(leased_until, '-infinity'::timestamptz)), goal_id
) WHERE priority;

-- +goose Down
DROP INDEX CONCURRENTLY study_plan_rebuilds_manual_ready_idx;
