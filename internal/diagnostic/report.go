package diagnostic

import "time"

type Report struct {
	Assigned int             `json:"assigned"`
	Assessed int             `json:"assessed"`
	Topics   []TopicCoverage `json:"topics"`
	Outcomes []AssessedTask  `json:"outcomes"`
}

type TopicCoverage struct {
	TopicID     string `json:"topic_id"`
	Assigned    int    `json:"assigned"`
	Assessed    int    `json:"assessed"`
	Observation string `json:"observation"`
	Uncertainty string `json:"uncertainty"`
}

type AssessedTask struct {
	TaskID            string    `json:"task_id"`
	VersionID         string    `json:"task_version_id"`
	TopicID           string    `json:"topic_id"`
	SubmissionID      string    `json:"submission_id"`
	EventID           string    `json:"event_id"`
	CorrectionVersion int64     `json:"correction_version"`
	Verdict           string    `json:"verdict"`
	GradedAt          time.Time `json:"graded_at"`
}

func buildReport(targets []string, tasks []Candidate, outcomes []AssessedTask) Report {
	report := Report{
		Assigned: len(tasks),
		Topics:   make([]TopicCoverage, 0, len(targets)+len(tasks)),
		Outcomes: make([]AssessedTask, 0, len(outcomes)),
	}
	positions := make(map[string]int, len(targets)+len(tasks))
	addTopic := func(id string) int {
		if i, ok := positions[id]; ok {
			return i
		}
		i := len(report.Topics)
		positions[id] = i
		report.Topics = append(report.Topics, TopicCoverage{TopicID: id, Observation: "unknown", Uncertainty: "no_evidence"})

		return i
	}
	for _, id := range targets {
		addTopic(id)
	}
	byTask := make(map[string]AssessedTask, len(outcomes))
	for _, item := range outcomes {
		byTask[item.TaskID] = item
	}
	for _, task := range tasks {
		row := &report.Topics[addTopic(task.TopicID)]
		row.Assigned++
		if outcome, ok := byTask[task.TaskID]; ok {
			row.Assessed++
			row.Observation = "not_demonstrated"
			if outcome.Verdict == "accepted" {
				row.Observation = "demonstrated_on_task"
			}
			row.Uncertainty = "limited"
			report.Outcomes = append(report.Outcomes, outcome)
		}
	}
	report.Assessed = len(report.Outcomes)

	return report
}
