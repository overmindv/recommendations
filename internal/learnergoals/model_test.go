package learnergoals

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func testID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func validGoal() Input {
	return Input{
		Title: "Графы", TemplateVersion: "algorithms:2", TargetTopics: []string{testID(1), testID(2)},
		WeeklyMinutes: 60, Active: true,
		Preferences: Preferences{
			Languages: []string{"go", "python"}, Formats: []string{"practice"},
			SelfAssessments: []SelfAssessment{{TopicID: testID(1), Level: "beginner"}},
		},
	}
}

func TestNormalizeLearnerGoal(t *testing.T) {
	in := validGoal()
	in.Title = "  Графы  "
	got, err := normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "Графы" || len(got.SelfAssessments) != 2 || got.SelfAssessments[1] != (SelfAssessment{testID(2), "unknown"}) {
		t.Error("normalization did not preserve the explicit unknown assessment")
	}
	for name, mutate := range map[string]func(*Input){
		"empty title":      func(in *Input) { in.Title = "  " },
		"title byte limit": func(in *Input) { in.Title = strings.Repeat("ц", 129) },
		"title NUL":        func(in *Input) { in.Title = "Go\x00" },
		"title newline":    func(in *Input) { in.Title = "Go\nPython" },
		"template":         func(in *Input) { in.TemplateVersion = "format has spaces" },
		"duplicate topic":  func(in *Input) { in.TargetTopics[1] = in.TargetTopics[0] },
		"invalid topic":    func(in *Input) { in.TargetTopics[0] = "not-a-uuid" },
		"active with zero": func(in *Input) { in.WeeklyMinutes = 0 },
		"too much time":    func(in *Input) { in.WeeklyMinutes = MaxWeeklyMinutes + 1 },
		"invalid date":     func(in *Input) { in.Deadline = "2026-02-29" },
		"duplicate lang":   func(in *Input) { in.Languages = []string{"go", "go"} },
		"invalid format":   func(in *Input) { in.Formats = []string{"../practice"} },
		"verified level":   func(in *Input) { in.SelfAssessments[0].Level = "mastered" },
		"unknown topic":    func(in *Input) { in.SelfAssessments[0].TopicID = testID(3) },
		"duplicate level":  func(in *Input) { in.SelfAssessments = append(in.SelfAssessments, in.SelfAssessments[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			in := validGoal()
			mutate(&in)
			if _, err := normalize(in); !errors.Is(err, ErrInvalid) {
				t.Error("expected validation error", err)
			}
		})
	}
}

func TestDeadlineUsesLearnerCalendar(t *testing.T) {
	in := validGoal()
	in.Deadline = "2026-11-18"
	now := time.Date(2026, 11, 19, 2, 0, 0, 0, time.UTC)
	if err := checkDeadline(in, Availability{AvailabilityInput: AvailabilityInput{Timezone: "America/Los_Angeles"}}, now); err != nil {
		t.Error("same local day was rejected", err)
	}
	if err := checkDeadline(in, Availability{AvailabilityInput: AvailabilityInput{Timezone: "UTC"}}, now); !errors.Is(err, ErrInvalid) {
		t.Error("past UTC date was accepted", err)
	}
	for _, zone := range []string{"", "Local", "../../etc/passwd", "Europe/NoSuchCity"} {
		if err := (AvailabilityInput{WeeklyMinutes: 120, Timezone: zone}).validate(); !errors.Is(err, ErrInvalid) {
			t.Error("invalid timezone accepted", zone)
		}
	}
}

func BenchmarkNormalizeLearnerGoal(b *testing.B) {
	in := validGoal()
	in.TargetTopics = make([]string, MaxTargetTopics)
	for i := range in.TargetTopics {
		in.TargetTopics[i] = testID(i + 1)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := normalize(in); err != nil {
			b.Fatal(err)
		}
	}
}
