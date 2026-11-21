-- +goose Up
ALTER TABLE diagnostic_sessions
    DROP CONSTRAINT diagnostic_sessions_status_check,
    ADD COLUMN finalized_at timestamptz,
    ADD COLUMN report jsonb,
    ADD CONSTRAINT diagnostic_sessions_status_check CHECK (status IN ('ready', 'no_tasks', 'skipped', 'finalized')),
    ADD CONSTRAINT diagnostic_sessions_report_check CHECK (
        ((status = 'finalized') = (finalized_at IS NOT NULL))
        AND ((finalized_at IS NOT NULL) = (report IS NOT NULL))
        AND (finalized_at IS NULL OR finalized_at >= started_at)
        AND (report IS NULL OR (jsonb_typeof(report) = 'object' AND octet_length(report::text) <= 8192))
    );

-- +goose Down
ALTER TABLE diagnostic_sessions DROP CONSTRAINT diagnostic_sessions_report_check;
UPDATE diagnostic_sessions SET status = 'skipped', skipped_at = finalized_at WHERE status = 'finalized';
ALTER TABLE diagnostic_sessions
    DROP CONSTRAINT diagnostic_sessions_status_check,
    DROP COLUMN report,
    DROP COLUMN finalized_at,
    ADD CONSTRAINT diagnostic_sessions_status_check CHECK (status IN ('ready', 'no_tasks', 'skipped'));
