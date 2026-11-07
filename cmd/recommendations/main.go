package main

import (
	"log/slog"
	"os"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/httpapi"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	os.Exit(parker.Main(func(app *parker.App) error {
		httpapi.Register(app.HTTP(), cfg.ServiceToken)
		return nil
	}, parker.WithAppName("recommendations"), func(o *parker.Options) { o.HTTPAddress = cfg.HTTPAddress }))
}
