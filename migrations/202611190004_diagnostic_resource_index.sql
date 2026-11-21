-- +goose Up
ALTER TABLE resource_projection_topics ADD COLUMN diagnostic_minutes smallint
    CHECK (diagnostic_minutes IS NULL OR (diagnostic_minutes BETWEEN 1 AND 20 AND origin = 'tasks'));

UPDATE resource_projection_topics AS t
SET diagnostic_minutes = (r.metadata->>'EstimatedMinutes')::smallint
FROM resource_projection_items AS r
WHERE t.origin = 'tasks' AND r.origin = t.origin
  AND r.generation = t.generation AND r.resource_id = t.resource_id
  AND (r.metadata->>'EstimatedMinutes')::integer BETWEEN 1 AND 20;

CREATE INDEX resource_projection_diagnostic_lookup
    ON resource_projection_topics (generation, topic_id, diagnostic_minutes, weight DESC, resource_id)
    WHERE origin = 'tasks' AND diagnostic_minutes IS NOT NULL;

-- +goose Down
ALTER TABLE resource_projection_topics DROP COLUMN diagnostic_minutes;
