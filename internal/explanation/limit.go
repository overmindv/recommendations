package explanation

import "github.com/overmindv/recommendations/internal/planner"

type Limit struct {
	TopicID string             `json:"topic_id"`
	Stage   planner.StudyStage `json:"stage,omitempty"`
	Code    string             `json:"code"`
	Message string             `json:"message"`
}

func Limits(outcomes []planner.UnmetOutcome) []Limit {
	limits := make([]Limit, 0, len(outcomes))
	for _, row := range outcomes {
		if !planner.ValidID(row.TopicID) {
			continue
		}
		limit := Limit{TopicID: row.TopicID}
		switch row.Stage {
		case planner.StudyTheory, planner.StudyPractice, planner.StudyCheck:
			limit.Stage = row.Stage
		}
		switch row.Reason {
		case "prerequisite":
			limit.Code, limit.Message = row.Reason, "Не удалось запланировать проверку по базовой теме."
		case "time_budget":
			limit.Code, limit.Message = row.Reason, "Для шага не хватает свободного времени до конца плана."
		case "missing_resource":
			limit.Code, limit.Message = row.Reason, "Нет доступного материала или задачи для этого шага."
		case "plan_limit":
			limit.Code, limit.Message = row.Reason, "В плане уже достигнут предел числа шагов."
		default:
			continue
		}
		limits = append(limits, limit)
	}
	return limits
}
