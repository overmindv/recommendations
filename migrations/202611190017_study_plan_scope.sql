-- +goose Up
ALTER TABLE study_plan_versions
    ADD COLUMN scope_topics uuid[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT study_plan_scope_size CHECK (
        cardinality(scope_topics) <= 64 AND array_position(scope_topics, NULL) IS NULL
    ) NOT VALID;

-- +goose Down
ALTER TABLE study_plan_versions DROP COLUMN scope_topics;
