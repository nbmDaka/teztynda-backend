package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/config"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
	"github.com/nbmDaka/teztynda-backend/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Worker failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.SetupLogger(cfg.AppEnv, cfg.LogLevel)
	log.Info("Starting Teztynda Background Worker", "env", cfg.AppEnv)

	var pgDB *storage.PostgresDB
	if cfg.PostgresDSN != "" {
		pgDB, err = storage.NewPostgresDB(cfg.PostgresDSN)
		if err != nil {
			log.Warn("Worker: PostgreSQL connection not established", "error", err)
		} else {
			defer pgDB.Close()
		}
	}

	var redisClient *storage.RedisClient
	if cfg.RedisURL != "" {
		redisClient, err = storage.NewRedisClient(cfg.RedisURL, cfg.RedisPassword, cfg.RedisDB)
		if err != nil {
			log.Warn("Worker: Redis connection not established", "error", err)
		} else {
			defer redisClient.Close()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Periodic maintenance ticker (e.g. session cleanup, telemetry)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runMaintenanceTasks(ctx, log, pgDB, redisClient)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Worker shutting down gracefully...")
	cancel()
	time.Sleep(500 * time.Millisecond)
	log.Info("Worker stopped cleanly")
}

func runMaintenanceTasks(ctx context.Context, log *slog.Logger, pgDB *storage.PostgresDB, redisClient *storage.RedisClient) {
	log.Debug("Running background worker maintenance cycle...")

	// 1. Mark stale active sessions (>24 hours old) as closed in PostgreSQL
	if pgDB != nil && pgDB.Pool != nil {
		query := `
		UPDATE sessions
		SET status = 'closed', closed_at = NOW()
		WHERE status = 'active' AND created_at < NOW() - INTERVAL '24 hours';
		`
		tag, err := pgDB.Pool.Exec(ctx, query)
		if err != nil {
			log.Error("Worker failed to prune stale sessions", "error", err)
		} else if tag.RowsAffected() > 0 {
			log.Info("Worker closed stale sessions", "count", tag.RowsAffected())
		}
	}
}
