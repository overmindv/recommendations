-- +goose Up
CREATE INDEX resource_projection_topics_weight
    ON resource_projection_topics (origin, generation, topic_id, weight DESC, resource_id);

-- +goose Down
DROP INDEX resource_projection_topics_weight;
