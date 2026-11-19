package planner

import (
	"testing"

	"github.com/overmindv/recommendations/internal/ranking"
)

func TestLearningDifficultyHintChangesPracticeButNotIndependentCheck(t *testing.T) {
	now, input := learningFixture()
	topic := studyID(1)
	knowledge := &ranking.Knowledge{ScoreBPS: 6500, Sufficient: true}
	input.TargetTopicIDs = []string{topic}
	input.Topics = map[string]ranking.Topic{topic: {Knowledge: knowledge}}
	input.Resources = studyTriplet(1, 10, 5)
	for i, difficulty := range []string{"medium", "hard"} {
		practice := testStudyResource(21+i, 1, StudyPractice, 5)
		check := testStudyResource(31+i, 1, StudyCheck, 5)
		practice.Difficulty, check.Difficulty = difficulty, difficulty
		input.Resources = append(input.Resources, practice, check)
	}
	for _, tc := range []struct{ hint, task int }{{-1, 11}, {1, 22}} {
		input.DifficultyHints = map[string]int{topic: tc.hint}
		plan, err := BuildLearningPlan(t.Context(), now, input)
		if err != nil || len(plan.Weeks) == 0 || len(plan.Weeks[0].Items) != 3 {
			t.Fatal("feedback prevented the usual stages:", err)
		}
		items := plan.Weeks[0].Items
		if items[1].ID != studyID(tc.task) || items[2].ID != studyID(31) || *knowledge != (ranking.Knowledge{ScoreBPS: 6500, Sufficient: true}) {
			t.Fatal("difficulty hint did not stay within practice:", items[1].ID, items[2].ID, knowledge)
		}
	}
}
