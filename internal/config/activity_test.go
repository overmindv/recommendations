package config

import (
	"testing"
	"time"
)

func TestActivityTTL(t *testing.T) {
	const oneHour = "1h"
	for _, value := range []string{"", oneHour, "720h", "0", "1s", "721h", "broken"} {
		t.Setenv("RECOMMENDATIONS_ACTIVITY_TTL", value)
		ttl, err := loadActivity(Config{Resources: Resources{Enabled: true}})
		valid := value == "" || value == oneHour || value == "720h"
		if (err == nil) != valid {
			t.Fatal(value, ttl, err)
		}
		if value == oneHour && ttl != time.Hour {
			t.Fatal(ttl)
		}
	}
	t.Setenv("RECOMMENDATIONS_ACTIVITY_TTL", oneHour)
	if _, err := loadActivity(Config{}); err == nil {
		t.Fatal("accepted absent projection")
	}
}
