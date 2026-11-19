package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/diagnostic"
	"github.com/overmindv/recommendations/internal/goals"
	"github.com/overmindv/recommendations/internal/httpapi"
	"github.com/overmindv/recommendations/internal/learnergoals"
	catalogprojection "github.com/overmindv/recommendations/internal/projection/catalog"
	"github.com/overmindv/recommendations/internal/projection/resources"
	"github.com/overmindv/recommendations/internal/savedplans"
	"github.com/overmindv/recommendations/internal/topiccatalog"
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
	return parker.Main(func(app *parker.App) error {
		var store goals.Store
		var plans savedplans.Store
		var onboarding learnergoals.Store
		var eligibility httpapi.ResourceCatalog
		var sessions *diagnostic.Service
		if source != nil && app.Config().DatabaseURL == "" {
			return errors.New("catalog projection requires DATABASE_URL")
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
			onboarding = learnerStore
			var diagnosticCatalog diagnostic.Catalog
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
				verified := resources.NewEligibility(projected, taskSource, materialSource)
				eligibility, diagnosticCatalog = verified, verified
			}
			sessions = diagnostic.NewService(learnerStore, diagnostic.NewPostgres(pool), diagnosticCatalog)
		}
		httpapi.RegisterVerifiedAI(app.HTTP(), cfg.ServiceToken, engine, eligibility)
		httpapi.RegisterGoals(app.HTTP(), cfg.ServiceToken, store)
		httpapi.RegisterVerifiedSavedPlans(app.HTTP(), cfg.ServiceToken, plans, eligibility)
		httpapi.RegisterOnboarding(app.HTTP(), cfg.ServiceToken, onboarding)
		httpapi.RegisterDiagnostic(app.HTTP(), cfg.ServiceToken, sessions)
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
