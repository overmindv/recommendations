package studyplans

import (
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/mastery"
)

const evidenceBasis = "evidence"

type Guidance struct {
	Basis               string `json:"basis"`
	TemplateVersion     string `json:"template_version"`
	VerifiedTopics      int    `json:"verified_topics"`
	RecommendDiagnostic bool   `json:"recommend_diagnostic"`
}

func studyGuidance(template string, evidence []mastery.Topic) Guidance {
	guide := Guidance{
		Basis: "template", TemplateVersion: learnergoals.StudyTemplateVersion(template), RecommendDiagnostic: true,
	}
	for _, topic := range evidence {
		if topic.Uncertainty == sufficientEvidence && topic.ScoreBPS != nil {
			guide.VerifiedTopics++
		}
	}
	if guide.VerifiedTopics > 0 {
		guide.Basis, guide.RecommendDiagnostic = evidenceBasis, false
	}

	return guide
}
