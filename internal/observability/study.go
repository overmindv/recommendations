package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/overmindv/parker"
	"github.com/prometheus/client_golang/prometheus"
)

type PlanOperation uint8

const (
	Generate PlanOperation = iota
	Rebuild
)

type PlanResult uint8

const (
	Saved PlanResult = iota
	Replayed
	Unchanged
	FallbackStale
	FallbackUnavailable
	FallbackBusy
	Conflict
	Rejected
	Unavailable
	Canceled
	Failed
)

var operationNames = [...]string{"generate", "rebuild"}
var resultNames = [...]string{"saved", "replayed", "unchanged", "fallback_stale",
	"fallback_unavailable", "fallback_busy", "conflict", "rejected", "unavailable", "canceled", "failed"}

type CatalogState uint8

const (
	CatalogUnavailable CatalogState = iota
	CatalogStale
	CatalogFresh
)

type Study struct {
	registry      *prometheus.Registry
	log           *slog.Logger
	plan          [len(operationNames)][len(resultNames)]prometheus.Observer
	candidates    [len(operationNames)][2]prometheus.Counter
	catalog       prometheus.Gauge
	checkpoints   [3]prometheus.Gauge
	committed     [len(evidenceNames)]prometheus.Counter
	lag           [len(evidenceNames)]prometheus.Observer
	evidenceError [len(failureNames)]prometheus.Counter
}

func NewStudy(log *slog.Logger, policyVersion, slot string, adaptive bool) *Study {
	m := &Study{registry: prometheus.NewRegistry(), log: log}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "recommendations_study_plan_duration_seconds",
		Help:    "Study plan generation or rebuild duration.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 4, 8, 12},
	}, []string{"operation", "result"})
	candidates := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recommendations_study_candidate_checks_total", Help: "Successful resource lookups by pool availability.",
	}, []string{"operation", "pool"})
	m.catalog = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "recommendations_study_catalog_state", Help: "Catalog state: 0 unavailable, 1 stale, 2 fresh.",
	})
	checkpoints := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recommendations_study_catalog_synced_timestamp_seconds", Help: "Last confirmed catalog synchronization, or 0 if unknown.",
	}, []string{"source"})
	policy := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "recommendations_study_ranking_policy_info", Help: "Ranking policy selected for new study plans.",
		ConstLabels: prometheus.Labels{"version": policyVersion, "slot": slot},
	})
	policy.Set(1)
	adaptation := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "recommendations_study_auto_adaptation_enabled", Help: "Whether automatic replanning from evidence is enabled.",
	})
	if adaptive {
		adaptation.Set(1)
	}
	m.registry.MustRegister(duration, candidates, m.catalog, checkpoints, policy, adaptation)
	for op, name := range operationNames {
		for result, value := range resultNames {
			m.plan[op][result] = duration.WithLabelValues(name, value)
		}
		m.candidates[op] = [2]prometheus.Counter{candidates.WithLabelValues(name, "empty"), candidates.WithLabelValues(name, "available")}
	}
	for i, name := range [...]string{"entities", "tasks", "content"} {
		m.checkpoints[i] = checkpoints.WithLabelValues(name)
	}
	m.registerEvidence()
	return m
}

func (m *Study) Plan(ctx context.Context, op PlanOperation, result PlanResult, elapsed time.Duration) {
	m.plan[op][result].Observe(elapsed.Seconds())
	if m.log != nil && (result == FallbackStale || result == FallbackUnavailable || result == FallbackBusy ||
		result == Unavailable || result == Failed) {
		m.log.DebugContext(ctx, "study plan decision", "operation", operationNames[op], "reason", resultNames[result],
			"trace_id", parker.TraceIDFrom(ctx))
	}
}

func (m *Study) Candidates(op PlanOperation, found bool) {
	if found {
		m.candidates[op][1].Inc()
	} else {
		m.candidates[op][0].Inc()
	}
}

func (m *Study) Catalog(state CatalogState, stamps [3]*time.Time) {
	for i, stamp := range stamps {
		if stamp == nil {
			m.checkpoints[i].Set(0)
		} else {
			m.checkpoints[i].Set(float64(stamp.Unix()))
		}
	}
	m.catalog.Set(float64(state))
}
