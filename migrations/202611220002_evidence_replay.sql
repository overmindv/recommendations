-- +goose Up
-- Старые inbox не содержат решения по event_id: NULL не разрешает новое архивирование.
ALTER TABLE learning_evidence_inbox ADD COLUMN rejected boolean;
ALTER TABLE learning_evidence_inbox ALTER COLUMN rejected SET DEFAULT false;
CREATE TABLE evidence_replay_clock (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 live_revision bigint NOT NULL DEFAULT 0,active_version uuid,
 building_version uuid,paused_until timestamptz
);
INSERT INTO evidence_replay_clock(singleton) VALUES(true);
CREATE TABLE evidence_replay_versions (
 id uuid PRIMARY KEY,state text NOT NULL CHECK(state IN ('preparing','building','active','cancelled','superseded')),
 live_revision bigint NOT NULL,manifest jsonb,through bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE evidence_replay_entries (
 version_id uuid NOT NULL REFERENCES evidence_replay_versions(id),position bigint NOT NULL,
 event_id uuid NOT NULL,digest bytea NOT NULL CHECK(octet_length(digest)=32),
 PRIMARY KEY(version_id,position),UNIQUE(version_id,event_id)
);
CREATE TABLE evidence_replay_outcomes (LIKE learning_task_outcomes INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE evidence_replay_outcomes ADD COLUMN version_id uuid NOT NULL REFERENCES evidence_replay_versions(id);
ALTER TABLE evidence_replay_outcomes ADD PRIMARY KEY(version_id,submission_id);
CREATE TABLE learning_outcome_history (
 event_id uuid PRIMARY KEY,submission_id uuid NOT NULL,learner_id uuid NOT NULL,
 correction_version bigint NOT NULL,proof jsonb NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX learning_outcome_history_subject ON learning_outcome_history(learner_id,submission_id,correction_version);
CREATE TABLE evidence_replay_history (LIKE learning_outcome_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE evidence_replay_history ADD COLUMN version_id uuid NOT NULL REFERENCES evidence_replay_versions(id);
ALTER TABLE evidence_replay_history ADD PRIMARY KEY(version_id,event_id);
ALTER TABLE evidence_replay_history ADD UNIQUE(version_id,submission_id,correction_version);
INSERT INTO learning_outcome_history(event_id,submission_id,learner_id,correction_version,proof)
 SELECT event_id,submission_id,learner_id,correction_version,to_jsonb(o) FROM learning_task_outcomes o;

-- +goose StatementBegin
CREATE FUNCTION retain_learning_outcome_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO learning_outcome_history(event_id,submission_id,learner_id,correction_version,proof)
 VALUES(NEW.event_id,NEW.submission_id,NEW.learner_id,NEW.correction_version,to_jsonb(NEW)) ON CONFLICT DO NOTHING;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER retain_learning_history AFTER INSERT OR UPDATE ON learning_task_outcomes FOR EACH ROW EXECUTE FUNCTION retain_learning_outcome_history();

-- +goose Down
ALTER TABLE learning_evidence_inbox DROP COLUMN rejected;
DROP TRIGGER retain_learning_history ON learning_task_outcomes;
DROP FUNCTION retain_learning_outcome_history();
DROP TABLE learning_outcome_history;
DROP TABLE evidence_replay_history;
DROP TABLE evidence_replay_outcomes;
DROP TABLE evidence_replay_entries;
DROP TABLE evidence_replay_versions;
DROP TABLE evidence_replay_clock;
