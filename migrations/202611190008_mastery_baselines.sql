-- +goose Up
ALTER TABLE learning_task_outcomes ADD COLUMN topic_weights integer[]
    CHECK (topic_weights IS NULL OR (
        cardinality(topic_weights) = cardinality(topic_ids)
        AND array_position(topic_weights, NULL) IS NULL
        AND 1 <= ALL(topic_weights) AND 10000 >= ALL(topic_weights)
    ));

CREATE TABLE mastery_policies (
    revision text PRIMARY KEY CHECK (octet_length(revision) BETWEEN 1 AND 64),
    configuration jsonb NOT NULL CHECK (jsonb_typeof(configuration) = 'object' AND octet_length(configuration::text) <= 1024)
);

CREATE TABLE mastery_baselines (
    goal_id uuid PRIMARY KEY REFERENCES learner_goals (id) ON DELETE CASCADE,
    goal_version bigint NOT NULL CHECK (goal_version > 0),
    policy_revision text NOT NULL REFERENCES mastery_policies (revision),
    computed_at timestamptz NOT NULL,
    topics jsonb NOT NULL CHECK (jsonb_typeof(topics) = 'array' AND jsonb_array_length(topics) BETWEEN 1 AND 16
        AND octet_length(topics::text) <= 16384),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'array' AND jsonb_array_length(evidence) <= 512
        AND octet_length(evidence::text) <= 524288),
    truncated boolean NOT NULL
);

-- +goose Down
DROP TABLE mastery_baselines;
DROP TABLE mastery_policies;
ALTER TABLE learning_task_outcomes DROP COLUMN topic_weights;
