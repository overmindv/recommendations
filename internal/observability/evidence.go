package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type EvidenceSource uint8

const (
	EvidenceTasks EvidenceSource = iota
	EvidenceContent
)

type EvidenceFailure uint8

const (
	EvidenceStoreFailure EvidenceFailure = iota
	EvidenceCommitFailure
	EvidencePollFailure
	EvidenceOffsetLost
	EvidenceAccessDenied
)

var evidenceNames = [...]string{"tasks", "content"}
var failureNames = [...]string{"store", "commit", "poll", "offset_lost", "access_denied"}

func (m *Study) registerEvidence() {
	committed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recommendations_learning_evidence_committed_total",
		Help: "Kafka records acknowledged after handling, including duplicates and rejections.",
	}, []string{"source"})
	lag := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "recommendations_learning_evidence_lag_seconds",
		Help:    "Age of Kafka records at successful consumer offset commit.",
		Buckets: []float64{.1, .5, 1, 5, 15, 30, 60, 120, 300, 900, 3600, 21600},
	}, []string{"source"})
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recommendations_learning_evidence_failures_total", Help: "Learning evidence failures by stage.",
	}, []string{"stage"})
	m.registry.MustRegister(committed, lag, failures)
	for i, name := range evidenceNames {
		m.committed[i] = committed.WithLabelValues(name)
		m.lag[i] = lag.WithLabelValues(name)
	}
	for i, name := range failureNames {
		m.evidenceError[i] = failures.WithLabelValues(name)
	}
}

func (m *Study) EvidenceCommitted(source EvidenceSource, recordTime, committedAt time.Time) {
	m.committed[source].Inc()
	if !recordTime.IsZero() {
		m.lag[source].Observe(max(0, committedAt.Sub(recordTime).Seconds()))
	}
}

func (m *Study) EvidenceFailure(reason EvidenceFailure) {
	m.evidenceError[reason].Inc()
}
