-- +goose Up
CREATE TABLE study_plan_explanations (
    goal_id uuid NOT NULL,
    plan_version bigint NOT NULL,
    ordinal smallint NOT NULL,
    details jsonb NOT NULL CHECK (jsonb_typeof(details) = 'object' AND octet_length(details::text) <= 4096),
    PRIMARY KEY (goal_id, plan_version, ordinal),
    FOREIGN KEY (goal_id, plan_version, ordinal)
        REFERENCES study_plan_items (goal_id, plan_version, ordinal) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE study_plan_explanations;
