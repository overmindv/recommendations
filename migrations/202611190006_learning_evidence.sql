-- +goose Up
CREATE TABLE learning_evidence_inbox (
    source text NOT NULL CHECK (source IN ('tasks', 'content')),
    event_id uuid NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event_id)
);

CREATE TABLE learning_evidence_rejections (
    topic text NOT NULL CHECK (char_length(topic) BETWEEN 1 AND 249),
    partition integer NOT NULL CHECK (partition >= 0),
    message_offset bigint NOT NULL CHECK (message_offset >= 0),
    reason text NOT NULL CHECK (reason IN ('invalid_event', 'event_id_conflict', 'outcome_conflict')),
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, message_offset)
);

CREATE TABLE learning_task_outcomes (
    submission_id uuid PRIMARY KEY,
    learner_id uuid NOT NULL,
    task_id uuid NOT NULL,
    task_version_id uuid NOT NULL,
    topic_ids uuid[] NOT NULL CHECK (cardinality(topic_ids) <= 8 AND array_position(topic_ids, NULL) IS NULL),
    verdict text NOT NULL CHECK (verdict IN ('accepted', 'wrong_answer', 'compilation_error',
        'runtime_error', 'time_limit_exceeded', 'memory_limit_exceeded', 'output_limit_exceeded')),
    evidence_kind text NOT NULL CHECK (evidence_kind IN ('independent', 'non_independent')),
    graded_at timestamptz NOT NULL,
    event_id uuid NOT NULL,
    correction_version bigint NOT NULL CHECK (correction_version > 0)
);

CREATE INDEX learning_task_independent_idx ON learning_task_outcomes (learner_id, task_id, graded_at, submission_id)
    WHERE evidence_kind = 'independent';

CREATE VIEW learning_independent_task_evidence AS
    SELECT DISTINCT ON (learner_id, task_id)
        learner_id, task_id, submission_id, task_version_id, topic_ids, verdict, graded_at, event_id, 1 AS evidence_weight
    FROM learning_task_outcomes
    WHERE evidence_kind = 'independent'
    ORDER BY learner_id, task_id, graded_at, submission_id;

CREATE TABLE learning_material_reads (
    learner_id uuid NOT NULL,
    material_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    confirmed_at timestamptz NOT NULL,
    event_id uuid NOT NULL,
    PRIMARY KEY (learner_id, material_id, revision_id),
    evidence_kind text NOT NULL DEFAULT 'self_reported' CHECK (evidence_kind = 'self_reported')
);

-- +goose Down
DROP TABLE learning_material_reads;
DROP VIEW learning_independent_task_evidence;
DROP TABLE learning_task_outcomes;
DROP TABLE learning_evidence_rejections;
DROP TABLE learning_evidence_inbox;
