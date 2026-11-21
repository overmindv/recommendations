-- +goose Up
ALTER TABLE study_plan_items
    ADD COLUMN superseded boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT study_plan_items_superseded_check CHECK (NOT superseded OR stage = 'independent_check') NOT VALID;

-- +goose Down
ALTER TABLE study_plan_items DROP COLUMN superseded;
