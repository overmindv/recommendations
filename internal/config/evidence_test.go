package config

import "testing"

func TestEvidenceConfig(t *testing.T) {
	setResourceEnv(t)
	t.Setenv("RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED", "false")
	t.Setenv("RECOMMENDATIONS_LEARNING_EVIDENCE_ENABLED", "")
	t.Setenv("KAFKA_CONTENT_LEARNING_TOPIC", "")
	if cfg, err := Load(); err != nil || cfg.Evidence.Enabled {
		t.Fatal("learning evidence not opt-in:", err)
	}
	t.Setenv("RECOMMENDATIONS_LEARNING_EVIDENCE_ENABLED", "true")
	if cfg, err := Load(); err != nil || !cfg.Evidence.Enabled || cfg.Evidence.ContentTopic != "content.learning.v1" {
		t.Fatal("valid local evidence configuration rejected:", err)
	}
	for _, topic := range []string{"learning.outcome.v1", "content.material.v1", "content.contribution.v1", "bad topic"} {
		t.Setenv("KAFKA_CONTENT_LEARNING_TOPIC", topic)
		if _, err := Load(); err == nil {
			t.Fatal("invalid evidence topic accepted:", topic)
		}
	}
	t.Setenv("KAFKA_CONTENT_LEARNING_TOPIC", "")
	t.Setenv("ENV", "production")
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	if _, err := Load(); err == nil {
		t.Fatal("anonymous evidence consumer allowed in production")
	}
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("KAFKA_SASL_USERNAME", "fixture")
	t.Setenv("KAFKA_SASL_PASSWORD", "fixture-value")
	if cfg, err := Load(); err != nil || len(cfg.Evidence.KafkaOptions) != 2 {
		t.Fatal("authenticated Kafka options not applied:", err)
	}
}
