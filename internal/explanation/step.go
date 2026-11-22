package explanation

import (
	"fmt"
	"slices"
	"time"

	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/planner"
	"github.com/overmindv/recommendations/internal/ranking"
)

type Code string

const (
	sufficientEvidence = "sufficient_for_policy"
	boundedEvidence    = "bounded_history"
	noEvidence         = "no_evidence"
)

const (
	TopicGap         Code = "topic_gap"
	Prerequisite     Code = "prerequisite"
	TimeFit          Code = "time_fit"
	DueReview        Code = "due_review"
	LimitedEvidence  Code = "insufficient_evidence"
	LearningSequence Code = "learning_sequence"
)

type Reason struct {
	Code    Code   `json:"code"`
	TopicID string `json:"topic_id,omitempty"`
	Minutes int    `json:"minutes,omitempty"`
	Message string `json:"message"`
}

type Evidence struct {
	TopicID        string       `json:"topic_id"`
	ScoreBPS       *int         `json:"score_bps,omitempty"`
	SampleCount    int          `json:"sample_count"`
	LastEvidenceAt *time.Time   `json:"last_evidence_at,omitempty"`
	Uncertainty    string       `json:"uncertainty"`
	Ref            *EvidenceRef `json:"ref,omitempty"`
}

type EvidenceRef struct {
	Watermark      string    `json:"watermark"`
	PolicyRevision string    `json:"policy_revision"`
	CapturedAt     time.Time `json:"captured_at"`
}

type Step struct {
	Reasons     []Reason                  `json:"reasons"`
	Evidence    Evidence                  `json:"evidence"`
	Alternative *planner.StudyAlternative `json:"alternative,omitempty"`
}

func ForStep(item planner.StudyItem, evidence *mastery.Topic) Step {
	summary := evidenceFor(item.TopicID, evidence)
	step := Step{Evidence: summary, Reasons: make([]Reason, 0, 4)}
	decision := item.Decision
	switch {
	case summary.Uncertainty != sufficientEvidence:
		step.Reasons = append(step.Reasons, Reason{Code: LimitedEvidence, TopicID: summary.TopicID})
	case decision != nil && summary.ScoreBPS != nil && *summary.ScoreBPS < decision.RequiredBPS:
		step.Reasons = append(step.Reasons, Reason{Code: TopicGap, TopicID: summary.TopicID})
	}
	if decision != nil && decision.ReviewDue && item.Stage == planner.StudyCheck {
		step.Reasons = append(step.Reasons, Reason{Code: DueReview, TopicID: summary.TopicID})
	}
	guards := slices.Clone(item.RequiresCheck)
	slices.Sort(guards)
	for _, id := range slices.Compact(guards) {
		if id != summary.TopicID && planner.ValidID(id) {
			step.Reasons = append(step.Reasons, Reason{Code: Prerequisite, TopicID: id})

			break
		}
	}
	if item.Minutes > 0 && item.Minutes <= ranking.MaxMinutes {
		step.Reasons = append(step.Reasons, Reason{Code: TimeFit, Minutes: item.Minutes})
	}
	if len(step.Reasons) == 0 {
		step.Reasons = append(step.Reasons, Reason{Code: LearningSequence})
	}
	for i := range step.Reasons {
		step.Reasons[i].Message = stepMessage(step.Reasons[i])
		if step.Reasons[i].Code == LimitedEvidence && summary.Uncertainty == "not_recorded" {
			step.Reasons[i].Message = "Для этой версии плана сводка оценок не сохранена."
		}
	}
	if decision != nil && item.Stage != planner.StudyCheck {
		alternative := decision.Alternative
		if alternative != nil && alternative.Kind == item.Kind && alternative.ID != item.ID &&
			planner.ValidID(alternative.ID) && planner.ValidID(alternative.RevisionID) &&
			alternative.Minutes > 0 && alternative.Minutes <= ranking.MaxMinutes {
			step.Alternative = new(*alternative)
		}
	}

	return step
}

func stepMessage(r Reason) string {
	switch r.Code {
	case TopicGap:
		return "Независимые проверки по теме пока ниже целевого уровня."
	case Prerequisite:
		return "Шаг доступен после проверки знаний по базовой теме."
	case TimeFit:
		return fmt.Sprintf("Шаг входит в недельный план. Оценка времени: %d мин.", r.Minutes)
	case DueReview:
		return "Пора повторно проверить знания по теме."
	case LimitedEvidence:
		return "Независимых оценок недостаточно для вывода о знании темы."
	default:
		return "Шаг продолжает работу по теме."
	}
}

func evidenceFor(id string, topic *mastery.Topic) Evidence {
	if !planner.ValidID(id) {
		id = ""
	}
	out := Evidence{TopicID: id, Uncertainty: "not_recorded"}
	if id == "" || topic == nil || topic.ID != id {
		return out
	}
	switch topic.Uncertainty {
	case "limited_sample", boundedEvidence, sufficientEvidence, noEvidence:
		out.Uncertainty = topic.Uncertainty
	default:
		return out
	}
	if out.Uncertainty == noEvidence || (out.Uncertainty == boundedEvidence && topic.SampleCount == 0) {
		return out
	}
	if topic.SampleCount < 1 || topic.SampleCount > 512 || topic.ScoreBPS == nil || *topic.ScoreBPS < 0 || *topic.ScoreBPS > 10000 ||
		topic.LastEvidenceAt == nil || topic.LastEvidenceAt.IsZero() {
		return Evidence{TopicID: id, Uncertainty: "not_recorded"}
	}
	out.SampleCount = topic.SampleCount
	out.ScoreBPS = new(*topic.ScoreBPS)
	out.LastEvidenceAt = new(topic.LastEvidenceAt.UTC())

	return out
}
