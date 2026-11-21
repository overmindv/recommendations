-- +goose Up
ALTER TABLE study_plan_versions
    ADD COLUMN scope_topics uuid[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT study_plan_scope_size CHECK (
        cardinality(scope_topics) <= 64 AND array_position(scope_topics, NULL) IS NULL
    ) NOT VALID;

ALTER TABLE mastery_baselines
    ADD COLUMN evidence_refs uuid[],
    ADD CONSTRAINT mastery_baseline_evidence_refs CHECK (
        evidence_refs IS NULL OR (
            cardinality(evidence_refs) <= 512 AND array_position(evidence_refs, NULL) IS NULL AND evidence = '[]'::jsonb
        )
    ) NOT VALID;

CREATE TABLE mastery_observed_evidence (
    goal_id uuid NOT NULL REFERENCES learner_goals (id) ON DELETE CASCADE,
    slot smallint NOT NULL CHECK (slot BETWEEN 1 AND 512),
    event_id uuid NOT NULL,
    submission_id uuid NOT NULL,
    proof jsonb NOT NULL CHECK (jsonb_typeof(proof) = 'object' AND octet_length(proof::text) <= 2048),
    PRIMARY KEY (goal_id, slot),
    UNIQUE (goal_id, event_id)
);

CREATE VIEW mastery_baseline_evidence AS
    SELECT b.goal_id,
        CASE WHEN b.evidence_refs IS NULL THEN b.evidence ELSE (
            SELECT COALESCE(jsonb_agg(f.proof ORDER BY refs.position), '[]'::jsonb)
            FROM unnest(b.evidence_refs) WITH ORDINALITY AS refs(event_id,position)
            JOIN mastery_observed_evidence f ON f.goal_id=b.goal_id AND f.event_id=refs.event_id
        ) END AS evidence
    FROM mastery_baselines b;

-- +goose Down
UPDATE mastery_baselines b SET evidence=e.evidence, evidence_refs=NULL
    FROM mastery_baseline_evidence e WHERE e.goal_id=b.goal_id AND b.evidence_refs IS NOT NULL;
DROP VIEW mastery_baseline_evidence;
ALTER TABLE mastery_baselines DROP COLUMN evidence_refs;
DROP TABLE mastery_observed_evidence;
ALTER TABLE study_plan_versions DROP COLUMN scope_topics;
