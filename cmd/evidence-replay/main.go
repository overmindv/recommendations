package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/recommendations/internal/config"
	"github.com/overmindv/recommendations/internal/replay"
)

const commandStart = "start"

func main() {
	if err := run(); err != nil {
		slog.Error("evidence replay failed; active model preserved unless activation committed")
		os.Exit(1)
	}
}
func run() error {
	if os.Getenv("RECOMMENDATIONS_REPLAY_ENABLED") != "true" || len(os.Args) < 2 || os.Getenv("DATABASE_URL") == "" {
		return errors.New("replay requires explicit configuration")
	}
	command := os.Args[1]
	id := ""
	if command != commandStart {
		if len(os.Args) != 3 || uuid.Validate(os.Args[2]) != nil {
			return replay.ErrSnapshot
		}
		id = os.Args[2]
	} else if len(os.Args) != 2 {
		return replay.ErrSnapshot
	}
	client, err := replay.NewClient(os.Getenv("TASKS_REPLAY_URL"), os.Getenv("TASKS_OUTCOME_REPLAY_TOKEN"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	store := replay.NewStore(pool, client)
	switch command {
	case commandStart:
		id, err = store.Start(ctx)
	case "step":
		err = store.Step(ctx, id)
	case "activate":
		cfg, loadErr := config.Load()
		if loadErr != nil {
			return loadErr
		}
		err = store.Activate(ctx, id, cfg.Mastery)
	case "cancel":
		err = store.Cancel(ctx, id)
	case "status":
	default:
		return replay.ErrSnapshot
	}
	if id != "" {
		status, statusErr := store.Status(ctx, id)
		if statusErr == nil {
			if encodeErr := json.NewEncoder(os.Stdout).Encode(status); encodeErr != nil {
				return encodeErr
			}
		} else if err == nil {
			return statusErr
		}
	}

	return err
}
