package config

import (
	"testing"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
)

func TestRetentionConfig(t *testing.T) {
	c := Config{Mastery: &mastery.Policy{}, Resources: Resources{Enabled: true}, Evidence: Evidence{Enabled: true}}
	t.Setenv("RECOMMENDATIONS_RETENTION_INTERVAL", "")
	if got, err := loadRetention(c); got != 0 || err != nil {
		t.Fatal("retention must be off without an interval:", got, err)
	}
	for _, input := range []string{"0s", "-1h", "5m", "1h1ms", "8761h", "not-a-duration"} {
		t.Setenv("RECOMMENDATIONS_RETENTION_INTERVAL", input)
		if _, err := loadRetention(c); err == nil {
			t.Fatal("invalid interval accepted:", input)
		}
	}
	t.Setenv("RECOMMENDATIONS_RETENTION_INTERVAL", "48h")
	if got, err := loadRetention(c); got != 48*time.Hour || err != nil {
		t.Fatal("explicit interval rejected:", got, err)
	}
	for _, missing := range []Config{
		{Resources: c.Resources, Evidence: c.Evidence},
		{Mastery: c.Mastery, Evidence: c.Evidence},
		{Mastery: c.Mastery, Resources: c.Resources},
	} {
		if _, err := loadRetention(missing); err == nil {
			t.Fatal("retention enabled with a missing dependency")
		}
	}
}
