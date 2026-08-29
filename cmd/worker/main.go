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
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
	"github.com/nbmDaka/teztynda-backend/internal/worker"
	"github.com/nbmDaka/teztynda-backend/pkg/logger"
)

func main() {
	// 1. Configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Worker failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Structured Logger
	log := logger.SetupLogger(cfg.AppEnv, cfg.LogLevel)
	log.Info("Starting Teztynda Background Worker", "env", cfg.AppEnv)

	// 3. PostgreSQL Initialization
	var pgDB *storage.PostgresDB
	if cfg.PostgresDSN != "" {
		pgDB, err = storage.NewPostgresDB(cfg.PostgresDSN)
		if err != nil {
			log.Warn("Worker: PostgreSQL connection not established", "error", err)
		} else {
			defer pgDB.Close()
		}
	}

	// 4. Redis Initialization
	var redisClient *storage.RedisClient
	if cfg.RedisURL != "" {
		redisClient, err = storage.NewRedisClient(cfg.RedisURL, cfg.RedisPassword, cfg.RedisDB)
		if err != nil {
			log.Warn("Worker: Redis connection not established", "error", err)
		} else {
			defer redisClient.Close()
		}
	}

	// 5. LLM Provider & Context Manager for Worker
	var llmProvider llm.LLMProvider
	if cfg.LLMProvider == "openai" && cfg.OpenAIAPIKey != "" {
		llmProvider = llm.NewOpenAIProvider(cfg.OpenAIAPIKey, cfg.OpenAIModel)
	} else {
		llmProvider = llm.NewFakeLLMProvider(100 * time.Millisecond)
	}
	llmService := llm.NewService(llmProvider)
	summarizer := ctxpkg.NewSummarizer(llmService)
	contextManager := ctxpkg.NewManager(
		redisClient,
		summarizer,
		cfg.MaxContextTokens,
		cfg.ShortMemoryTokens,
		cfg.SessionTTL,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 6. Launch Redis Queue Summarization Worker
	summarizationWorker := worker.NewSummarizationWorker(redisClient, contextManager)
	go summarizationWorker.Run(ctx)

	// 7. Periodic Maintenance Ticker (e.g. prune stale sessions in DB)
	maintenanceTicker := time.NewTicker(1 * time.Minute)
	defer maintenanceTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-maintenanceTicker.C:
				runMaintenanceTasks(ctx, log, pgDB)
			}
		}
	}()

	// 8. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Worker shutting down gracefully...")
	cancel()
	time.Sleep(500 * time.Millisecond)
	log.Info("Worker stopped cleanly")
}

func runMaintenanceTasks(ctx context.Context, log *slog.Logger, pgDB *storage.PostgresDB) {
	log.Debug("Running background maintenance tasks...")

	if pgDB != nil && pgDB.Pool != nil {
		query := `
		UPDATE sessions
		SET status = 'closed', closed_at = NOW()
		WHERE status = 'active' AND created_at < NOW() - INTERVAL '24 hours';
		`
		tag, err := pgDB.Pool.Exec(ctx, query)
		if err != nil {
			log.Error("Worker failed to prune stale sessions in PostgreSQL", "error", err)
		} else if tag.RowsAffected() > 0 {
			log.Info("Worker marked stale sessions as closed", "count", tag.RowsAffected())
		}
	}
}
