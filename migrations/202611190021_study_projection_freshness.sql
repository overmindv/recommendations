-- +goose Up
ALTER TABLE catalog_projection_state ADD COLUMN last_synced_at timestamptz;
ALTER TABLE resource_projection_checkpoints ADD COLUMN last_synced_at timestamptz;

-- +goose Down
ALTER TABLE resource_projection_checkpoints DROP COLUMN last_synced_at;
ALTER TABLE catalog_projection_state DROP COLUMN last_synced_at;
