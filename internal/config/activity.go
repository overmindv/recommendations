package config

import (
	"errors"
	"os"
	"strings"
	"time"
)

func loadActivity(c Config) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("RECOMMENDATIONS_ACTIVITY_TTL"))
	if value == "" {
		return 0, nil
	}
	ttl, err := time.ParseDuration(value)
	if err != nil || ttl < time.Hour || ttl > 720*time.Hour || ttl%time.Second != 0 {
		return 0, errors.New("RECOMMENDATIONS_ACTIVITY_TTL must be 1h to 720h in whole seconds")
	}
	if !c.Resources.Enabled {
		return 0, errors.New("study activity requires resource projection")
	}

	return ttl, nil
}
