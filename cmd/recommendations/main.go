package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/completion"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/consumer"
	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/overmindv/recommendations/internal/goals"
	"github.com/overmindv/recommendations/internal/httpapi"
	"github.com/overmindv/recommendations/internal/learnergoals"
	"github.com/overmindv/recommendations/internal/mastery"
	"github.com/overmindv/recommendations/internal/outbox"
	catalogprojection "github.com/overmindv/recommendations/internal/projection/catalog"
	"github.com/overmindv/recommendations/internal/projection/resources"
	"github.com/overmindv/recommendations/internal/savedplans"
	"github.com/overmindv/recommendations/internal/studyplans"
	"github.com/overmindv/recommendations/internal/topiccatalog"
	"github.com/overmindv/recommendations/pkg/api/goalcompletion"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		return parker.Main(nil, parker.WithAppName("recommendations"))
	}
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return 1
	}
	engine, err := aiplan.FromEnv()
	if err != nil {
		slog.Error("invalid AI configuration", "error", err)
		return 1
	}
	catalog, err := topiccatalog.New(cfg.EntitiesURL)
	if err != nil {
		slog.Error("invalid catalog configuration", "error", err)
		return 1
	}
	var source *catalogprojection.Client
	if cfg.CatalogSnapshotToken != "" {
		source, err = catalogprojection.NewClient(cfg.EntitiesURL, cfg.CatalogSnapshotToken)
		if err != nil {
			slog.Error("invalid catalog projection configuration", "error", err)
			return 1
		}
	}
	var taskSource *resources.TaskClient
	var materialSource *resources.ContentClient
	var materialLog *resources.MaterialLog
	if cfg.Resources.Enabled {
		if taskSource, err = resources.NewTaskClient(cfg.Resources.TasksURL, cfg.Resources.TasksToken); err != nil {
			slog.Error("invalid tasks catalog configuration", "error", err)
			return 1
		}
		defer taskSource.CloseIdleConnections()
		if materialSource, err = resources.NewContentClient(cfg.Resources.ContentAddress, cfg.Resources.ContentToken); err != nil {
			slog.Error("invalid content catalog configuration", "error", err)
			return 1
		}
		defer func() { _ = materialSource.Close() }()
		options := append([]kgo.Opt{kgo.SeedBrokers(parker.EnvList("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")...)}, cfg.Resources.KafkaOptions...)
		if materialLog, err = resources.NewMaterialLog(cfg.Resources.MaterialTopic, options...); err != nil {
			slog.Error("invalid material log configuration", "error", err)
			return 1
		}
		defer materialLog.Close()
	}
	var goalProducer *kgo.Client
	if cfg.StudyPlans != nil && cfg.StudyPlans.CompletionEvents {
		options := append([]kgo.Opt{
			kgo.SeedBrokers(parker.EnvList("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")...),
			kgo.DefaultProduceTopic(goalcompletion.Topic), kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.RecordDeliveryTimeout(8 * time.Second),
		}, cfg.Evidence.KafkaOptions...)
		goalProducer, err = kgo.NewClient(options...)
		if err != nil {
			slog.Error("invalid goal completion producer configuration")
			return 1
		}
		defer goalProducer.Close()
	}
	return parker.Main(func(app *parker.App) error {
		var store goals.Store
		var plans savedplans.Store
		var onboarding learnergoals.Store
		var eligibility httpapi.ResourceCatalog
		var sessions *diagnostic.Service
		var baselines *mastery.Postgres
		var retention *mastery.Retention
		var studyService httpapi.StudyPlans
		if source != nil && app.Config().DatabaseURL == "" {
			return errors.New("catalog projection requires DATABASE_URL")
		}
		if cfg.Evidence.Enabled && app.Config().DatabaseURL == "" {
			return errors.New("learning evidence requires DATABASE_URL")
		}
		if cfg.Mastery != nil && app.Config().DatabaseURL == "" {
			return errors.New("mastery baseline requires DATABASE_URL")
		}
		if app.Config().DatabaseURL != "" {
			pool, err := app.Postgres()
			if err != nil {
				return err
			}
			store = goals.NewPostgres(pool)
			plans = &savedplans.Postgres{Pool: pool}
			learnerStore := learnergoals.NewValidatedPostgres(pool, func(ctx context.Context, input learnergoals.Input) error {
				if err := learnergoals.ValidateTemplate(input.TemplateVersion); err != nil {
					return err
				}
				return catalog.Validate(ctx, input.TargetTopics)
			})
			if goalProducer != nil {
				learnerStore = learnerStore.WithCompletionChecks()
				app.AddRunnable("goal-completion-checks", completion.NewWorker(pool, app.Logger()).Run)
				app.AddRunnable("goal-completion-outbox", outbox.NewGoalPublisher(pool, goalProducer, app.Logger()).Run)
			}
			onboarding = learnerStore
			var diagnosticCatalog diagnostic.Catalog
			var verified *resources.Eligibility
			app.AddRunnable("learner-goal-requests", pruneGoalRequests(app.Logger(), learnerStore))
			if source != nil {
				worker := catalogprojection.NewWorker(catalogprojection.NewStore(pool), source, app.Logger())
				app.AddRunnable("catalog-projection", worker.Run)
			}
			if cfg.Resources.Enabled {
				projected := resources.NewStore(pool)
				app.AddHealthCheck("resource-projection", parker.HealthCheckFunc(projected.Ready))
				app.AddRunnable("task-resource-projection", resources.NewTasksWorker(projected, taskSource, app.Logger()).Run)
				app.AddRunnable("material-resource-projection", resources.NewContentWorker(projected, materialSource, materialLog, app.Logger()).Run)
				verified = resources.NewEligibility(projected, taskSource, materialSource)
				eligibility, diagnosticCatalog = verified, verified
			}
			sessions = diagnostic.NewService(learnerStore, diagnostic.NewPostgres(pool), diagnosticCatalog)
			if cfg.Mastery != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				baselines, err = mastery.NewPostgres(ctx, pool, *cfg.Mastery)
				cancel()
				if err != nil {
					return err
				}
				if cfg.RetentionInterval > 0 {
					retention = mastery.NewRetention(baselines, verified, cfg.RetentionInterval)
				}
			}
			if cfg.Evidence.Enabled {
				evidenceStore := consumer.NewPostgres(pool)
				if cfg.StudyPlans != nil && cfg.StudyPlans.AdaptationWindow > 0 {
					evidenceStore, err = consumer.NewPostgresWithPlanChanges(pool, cfg.StudyPlans.AdaptationWindow, baselines)
					if err != nil {
						return err
					}
				}
				if goalProducer != nil {
					evidenceStore = evidenceStore.WithCompletionChecks()
				}
				options := append([]kgo.Opt{kgo.SeedBrokers(parker.EnvList("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")...)}, cfg.Evidence.KafkaOptions...)
				worker := consumer.NewWorker(evidenceStore, cfg.Evidence.ContentTopic, app.Logger(), options...)
				app.AddRunnable("learning-evidence", worker.Run)
			}
			if cfg.StudyPlans != nil {
				planSource := resources.NewStudySource(verified, cfg.StudyPlans.Recent)
				builder := studyplans.NewProjectedBuilder(pool, planSource, cfg.StudyPlans.Ranking, *cfg.Mastery)
				studyStore := studyplans.NewPostgres(pool)
				if goalProducer != nil {
					studyStore = studyStore.WithCompletionChecks()
				}
				studyService = studyplans.NewService(studyStore, builder)
				app.AddRunnable("study-plan-adaptation", studyplans.NewRebuildWorker(studyStore, builder, app.Logger()).Run)
			}
		}
		httpapi.RegisterVerifiedAI(app.HTTP(), cfg.ServiceToken, engine, eligibility)
		httpapi.RegisterGoals(app.HTTP(), cfg.ServiceToken, store)
		httpapi.RegisterVerifiedSavedPlans(app.HTTP(), cfg.ServiceToken, plans, eligibility)
		httpapi.RegisterOnboarding(app.HTTP(), cfg.ServiceToken, onboarding)
		httpapi.RegisterDiagnostic(app.HTTP(), cfg.ServiceToken, sessions)
		httpapi.RegisterMastery(app.HTTP(), cfg.ServiceToken, baselines)
		httpapi.RegisterRetention(app.HTTP(), cfg.ServiceToken, retention)
		httpapi.RegisterStudyPlans(app.HTTP(), cfg.ServiceToken, studyService)
		return nil
	}, parker.WithAppName("recommendations"), func(o *parker.Options) { o.HTTPAddress = cfg.HTTPAddress; o.WriteTimeout = 35 * time.Second })
}

func pruneGoalRequests(log *slog.Logger, store *learnergoals.Postgres) parker.Runnable {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				pruneCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := store.PruneExpiredRequests(pruneCtx, 1000)
				cancel()
				if err != nil && ctx.Err() == nil {
					log.Warn("could not prune expired learner goal keys")
				}
			}
		}
	}
}
