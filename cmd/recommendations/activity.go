package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/overmindv/parker"
	"github.com/overmindv/recommendations/internal/studyactivity"
)

func pruneActivity(log *slog.Logger, store *studyactivity.Postgres) parker.Runnable {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				pruneCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := store.Prune(pruneCtx)
				cancel()
				if err != nil && ctx.Err() == nil {
					log.Warn("could not prune expired study activity")
				}
			}
		}
	}
}
