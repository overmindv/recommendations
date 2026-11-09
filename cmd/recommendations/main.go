package main

import (
	"log/slog"
	"os"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/goals"
	"github.com/overmindv/recommendations/internal/httpapi"
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
	os.Exit(parker.Main(func(app *parker.App) error {
		httpapi.Register(app.HTTP(), cfg.ServiceToken)
		var store goals.Store
		if app.Config().DatabaseURL != "" {
			pool, err := app.Postgres()
			if err != nil {
				return err
			}
			store = goals.NewPostgres(pool)
		}
		httpapi.RegisterGoals(app.HTTP(), cfg.ServiceToken, store)
		return nil
	}, parker.WithAppName("recommendations"), func(o *parker.Options) { o.HTTPAddress = cfg.HTTPAddress }))
}
