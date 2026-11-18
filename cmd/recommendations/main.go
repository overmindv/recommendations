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
	"github.com/overmindv/recommendations/internal/goals"
	"github.com/overmindv/recommendations/internal/httpapi"
	"github.com/overmindv/recommendations/internal/learnergoals"
	catalogprojection "github.com/overmindv/recommendations/internal/projection/catalog"
	"github.com/overmindv/recommendations/internal/savedplans"
	"github.com/overmindv/recommendations/internal/topiccatalog"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		os.Exit(parker.Main(nil, parker.WithAppName("recommendations")))
	}
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	engine, err := aiplan.FromEnv()
	if err != nil {
		slog.Error("invalid AI configuration", "error", err)
		os.Exit(1)
	}
	catalog, err := topiccatalog.New(cfg.EntitiesURL)
	if err != nil {
		slog.Error("invalid catalog configuration", "error", err)
		os.Exit(1)
	}
	var source *catalogprojection.Client
	if cfg.CatalogSnapshotToken != "" {
		source, err = catalogprojection.NewClient(cfg.EntitiesURL, cfg.CatalogSnapshotToken)
		if err != nil {
			slog.Error("invalid catalog projection configuration", "error", err)
			os.Exit(1)
		}
	}
	os.Exit(parker.Main(func(app *parker.App) error {
		httpapi.RegisterAI(app.HTTP(), cfg.ServiceToken, engine)
		var store goals.Store
		var plans savedplans.Store
		var onboarding learnergoals.Store
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
			app.AddRunnable("learner-goal-requests", pruneGoalRequests(app.Logger(), learnerStore))
			if source != nil {
				worker := catalogprojection.NewWorker(catalogprojection.NewStore(pool), source, app.Logger())
				app.AddRunnable("catalog-projection", worker.Run)
			}
		}
		httpapi.RegisterGoals(app.HTTP(), cfg.ServiceToken, store)
		httpapi.RegisterSavedPlans(app.HTTP(), cfg.ServiceToken, plans)
		httpapi.RegisterOnboarding(app.HTTP(), cfg.ServiceToken, onboarding)
		return nil
	}, parker.WithAppName("recommendations"), func(o *parker.Options) { o.HTTPAddress = cfg.HTTPAddress; o.WriteTimeout = 35 * time.Second }))
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
