-- +goose Up
CREATE TABLE saved_learning_plans (
 id uuid PRIMARY KEY,
 owner_id uuid NOT NULL,
 document jsonb NOT NULL,
 is_public boolean NOT NULL DEFAULT false,
 revision bigint NOT NULL DEFAULT 1,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX saved_learning_plans_owner_idx ON saved_learning_plans(owner_id, updated_at DESC);

-- +goose Down
DROP TABLE saved_learning_plans;
