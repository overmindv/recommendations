package config

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
)

func loadRetention(c Config) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("RECOMMENDATIONS_RETENTION_INTERVAL"))
	if value == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval < mastery.MinReviewInterval || interval > mastery.MaxReviewInterval ||
		interval%time.Second != 0 {
		return 0, errors.New("RECOMMENDATIONS_RETENTION_INTERVAL must be a duration from 1h to 8760h in whole seconds")
	}
	if c.Mastery == nil || !c.Evidence.Enabled || !c.Resources.Enabled {
		return 0, errors.New("retention requires mastery, learning evidence and resource projection")
	}

	return interval, nil
}
