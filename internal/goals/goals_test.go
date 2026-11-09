package goals

import (
	"errors"
	"testing"
)

func TestValidateGoal(t *testing.T) {
	valid := Input{TopicIDs: []string{"00000000-0000-0000-0000-000000000001"}, Minutes: 60}
	if err := Validate(valid); err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]Input{
		"empty topics":     {Minutes: 60},
		"duplicate topics": {TopicIDs: []string{valid.TopicIDs[0], valid.TopicIDs[0]}, Minutes: 60},
		"invalid topic":    {TopicIDs: []string{"other"}, Minutes: 60},
		"low budget":       {TopicIDs: valid.TopicIDs, Minutes: 9},
		"high budget":      {TopicIDs: valid.TopicIDs, Minutes: 241},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected invalid goal, got %v", err)
			}
		})
	}
}
