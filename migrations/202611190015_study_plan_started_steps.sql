-- +goose Up
ALTER TABLE study_plan_activity
    ALTER COLUMN completed_at DROP NOT NULL,
    ADD COLUMN started_at timestamptz,
    ADD CONSTRAINT study_plan_activity_times CHECK (
        (started_at IS NOT NULL OR completed_at IS NOT NULL)
        AND (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
    ) NOT VALID;

ALTER TABLE study_plan_items ADD COLUMN evidence_since timestamptz;

-- +goose Down
ALTER TABLE study_plan_items DROP COLUMN evidence_since;
ALTER TABLE study_plan_activity
    DROP CONSTRAINT study_plan_activity_times,
    DROP COLUMN started_at,
    ALTER COLUMN completed_at SET NOT NULL;
