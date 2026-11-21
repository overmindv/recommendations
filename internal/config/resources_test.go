package config

import (
	"strings"
	"testing"
)

func setResourceEnv(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"ENV": "test", "RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED": "true",
		"RECOMMENDATIONS_SERVICE_TOKEN": strings.Repeat("s", 32), "HTTP_ADDRESS": ":8080",
		"ENTITIES_URL": "http://entities:8080", "ENTITIES_SNAPSHOT_TOKEN": strings.Repeat("e", 32),
		"TASKS_RESOURCE_URL": "http://tasks:8080", "TASKS_RESOURCE_TOKEN": strings.Repeat("t", 32),
		"CONTENT_CATALOG_ADDRESS": "content:9090", "CONTENT_GRPC_CATALOG_TOKEN": strings.Repeat("c", 32),
		"KAFKA_CONTENT_MATERIAL_TOPIC": "", "KAFKA_TLS_ENABLED": "", "KAFKA_TLS_CA_FILE": "",
		"KAFKA_TLS_CERT_FILE": "", "KAFKA_TLS_KEY_FILE": "", "KAFKA_TLS_SERVER_NAME": "",
		"KAFKA_SASL_MECHANISM": "", "KAFKA_SASL_USERNAME": "", "KAFKA_SASL_PASSWORD": "",
	} {
		t.Setenv(key, value)
	}
}

func TestResourceProjectionConfigRequiresAllSources(t *testing.T) {
	setResourceEnv(t)
	c, loadErr := Load()
	if loadErr != nil || !c.Resources.Enabled || c.Resources.MaterialTopic != defaultMaterialTopic {
		t.Fatal("local fixture could not enable resource projection:", loadErr)
	}
	t.Setenv("CONTENT_GRPC_CATALOG_TOKEN", strings.Repeat("t", 32))
	if _, err := Load(); err == nil {
		t.Fatal("shared catalog credentials accepted")
	}
	t.Setenv("CONTENT_CATALOG_ADDRESS", "")
	if _, err := Load(); err == nil {
		t.Fatal("missing source accepted")
	}
	t.Setenv("RECOMMENDATIONS_RESOURCE_PROJECTION_ENABLED", "false")
	disabled, err := Load()
	if err != nil || disabled.Resources.Enabled {
		t.Fatal("disabled projection changed service configuration:", err)
	}
}

func TestResourceKafkaProductionRequiresVerifiedTLS(t *testing.T) {
	setResourceEnv(t)
	t.Setenv("ENV", "production")
	if _, err := Load(); err == nil {
		t.Fatal("production allowed plaintext catalog")
	}
	t.Setenv("ENTITIES_URL", "https://entities:443")
	t.Setenv("TASKS_RESOURCE_URL", "https://tasks:443")
	t.Setenv("CONTENT_CATALOG_ADDRESS", "grpcs://content:443")
	if _, err := Load(); err == nil {
		t.Fatal("production allowed plaintext Kafka")
	}
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("KAFKA_SASL_USERNAME", "fixture-user")
	t.Setenv("KAFKA_SASL_PASSWORD", strings.Repeat("p", 32))
	c, err := Load()
	if err != nil || len(c.Resources.KafkaOptions) != 2 {
		t.Fatal("valid Kafka TLS and SASL settings were not applied:", err)
	}
	t.Setenv("KAFKA_TLS_CERT_FILE", "unpaired-fixture")
	if _, err := Load(); err == nil {
		t.Fatal("unpaired TLS client certificate accepted")
	}
}
