package config

import "testing"

func TestMasteryRequiresExplicitPolicy(t *testing.T) {
	t.Setenv("RECOMMENDATIONS_MASTERY_ENABLED", "")
	if p, err := loadMastery(); err != nil || p != nil {
		t.Fatal("mastery must be disabled by default:", p, err)
	}
	t.Setenv("RECOMMENDATIONS_MASTERY_ENABLED", "true")
	t.Setenv("RECOMMENDATIONS_MASTERY_POLICY_REVISION", "test-r1")
	t.Setenv("RECOMMENDATIONS_MASTERY_GAP_MAX_BPS", "3000")
	t.Setenv("RECOMMENDATIONS_MASTERY_DEMONSTRATED_MIN_BPS", "8000")
	t.Setenv("RECOMMENDATIONS_MASTERY_MIN_WEIGHTED_TASKS", "")
	if _, err := loadMastery(); err == nil {
		t.Fatal("missing threshold was accepted")
	}
	t.Setenv("RECOMMENDATIONS_MASTERY_MIN_WEIGHTED_TASKS", "1")
	if p, err := loadMastery(); err != nil || p == nil || !p.Valid() {
		t.Fatal("explicit valid policy was rejected:", p, err)
	}
	t.Setenv("RECOMMENDATIONS_MASTERY_GAP_MAX_BPS", "8000")
	if _, err := loadMastery(); err == nil {
		t.Fatal("overlapping thresholds were accepted")
	}
}
