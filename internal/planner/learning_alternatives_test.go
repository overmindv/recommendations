package planner

import "testing"

func TestLearningAlternativesFitAfterAllocationAndDoNotReuseChecks(t *testing.T) {
	t.Parallel()

	now, input := learningFixture()
	topic := studyID(1)
	input.TargetTopicIDs = []string{topic}
	input.Budget.GoalMinutes, input.Budget.MaxWeeks = 30, 1
	input.Resources = studyTriplet(1, 10, 10)
	input.Resources = append(input.Resources,
		testStudyResource(22, 1, StudyTheory, 20),
		testStudyResource(12, 1, StudyPractice, 10),
		testStudyResource(14, 1, StudyCheck, 10),
		testStudyResource(15, 1, StudyPractice, 10),
	)
	plan, err := BuildLearningPlan(t.Context(), now, input)
	if err != nil || len(plan.Weeks) != 1 || len(plan.Weeks[0].Items) != 3 {
		t.Fatal("fixture did not schedule three stages:", err)
	}
	theory, practice, check := plan.Weeks[0].Items[0], plan.Weeks[0].Items[1], plan.Weeks[0].Items[2]
	if theory.Decision.Alternative != nil || practice.Decision.Alternative == nil || practice.Decision.Alternative.ID != studyID(15) ||
		check.ID != studyID(12) || check.Decision.Alternative != nil {
		t.Fatal("alternative used time reserved for another step or reused a check:", theory.Decision, practice.Decision, check)
	}
}
