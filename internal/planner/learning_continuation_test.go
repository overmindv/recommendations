package planner

import (
	"testing"

	"github.com/overmindv/recommendations/internal/ranking"
)

func BenchmarkContinueLearningPlan(b *testing.B) {
	now, request := learningFixture()
	request.Budget.MaxWeeks = MaxLearningWeeks
	request.TargetTopicIDs = []string{studyID(1)}
	request.Topics = make(map[string]ranking.Topic)
	request.Resources = nil
	var prerequisites []string
	for topic := 1; topic <= 64; topic++ {
		request.Topics[studyID(topic)] = ranking.Topic{}
		if topic != 1 {
			prerequisites = append(prerequisites, studyID(topic))
		}
		for i := range 10 {
			request.Resources = append(request.Resources, studyTriplet(topic, topic*100+i*3, 20)...)
		}
	}
	request.Topics[studyID(1)] = ranking.Topic{Prerequisites: prerequisites}
	base, err := BuildLearningPlan(b.Context(), now, request)
	if err != nil || len(base.Weeks) == 0 || len(base.Weeks[0].Items) == 0 {
		b.Fatal("benchmark needs a preserved item:", err)
	}
	kept := LearningContinuation{Weeks: make([]LearningWeek, len(base.Weeks))}
	for i, week := range base.Weeks {
		kept.Weeks[i].StudyWeek = week.StudyWeek
	}
	kept.Weeks[0].Items = base.Weeks[0].Items[:1]
	b.ReportAllocs()
	for b.Loop() {
		_, err := ContinueLearningPlan(b.Context(), now, request, kept)
		if err != nil {
			b.Fatal(err)
		}
	}
}
