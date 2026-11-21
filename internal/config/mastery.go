package config

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/overmindv/recommendations/internal/mastery"
)

func loadMastery() (*mastery.Policy, error) {
	enabled, err := envSwitch("RECOMMENDATIONS_MASTERY_ENABLED")
	if err != nil || !enabled {
		return nil, err
	}
	parse := func(name string) (int, error) {
		value, parseErr := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
		if parseErr != nil {
			return 0, errors.New("invalid or missing " + name)
		}

		return value, nil
	}
	p := &mastery.Policy{Revision: strings.TrimSpace(os.Getenv("RECOMMENDATIONS_MASTERY_POLICY_REVISION")), Algorithm: mastery.AlgorithmVersion}
	if p.GapMaxBPS, err = parse("RECOMMENDATIONS_MASTERY_GAP_MAX_BPS"); err != nil {
		return nil, err
	}
	if p.DemonstratedBPS, err = parse("RECOMMENDATIONS_MASTERY_DEMONSTRATED_MIN_BPS"); err != nil {
		return nil, err
	}
	if p.MinWeightedTasks, err = parse("RECOMMENDATIONS_MASTERY_MIN_WEIGHTED_TASKS"); err != nil {
		return nil, err
	}
	if !p.Valid() {
		return nil, errors.New("invalid mastery policy revision or thresholds")
	}

	return p, nil
}
