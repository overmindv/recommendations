package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/aiplan"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/goals"
	"github.com/overmindv/recommendations/internal/httpapi"
	"github.com/overmindv/recommendations/internal/savedplans"
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
	os.Exit(parker.Main(func(app *parker.App) error {
		httpapi.RegisterAI(app.HTTP(), cfg.ServiceToken, engine)
		var store goals.Store
		var plans savedplans.Store
		if app.Config().DatabaseURL != "" {
			pool, err := app.Postgres()
			if err != nil {
				return err
			}
			store = goals.NewPostgres(pool)
			plans = &savedplans.Postgres{Pool: pool}
		}
		httpapi.RegisterGoals(app.HTTP(), cfg.ServiceToken, store)
		httpapi.RegisterSavedPlans(app.HTTP(), cfg.ServiceToken, plans)
		return nil
	}, parker.WithAppName("recommendations"), func(o *parker.Options) { o.HTTPAddress = cfg.HTTPAddress; o.WriteTimeout = 35 * time.Second }))
}
