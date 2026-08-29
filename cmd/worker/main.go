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
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/session"
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

	// 5. LLM Provider & Memory Manager for Worker
	var llmProvider llm.LLMProvider
	if cfg.LLMProvider == "openai" && cfg.OpenAIAPIKey != "" {
		llmProvider = llm.NewOpenAIProvider(cfg.OpenAIAPIKey, cfg.OpenAIModel)
	} else {
		llmProvider = llm.NewFakeLLMProvider(100 * time.Millisecond)
	}
	llmService := llm.NewService(llmProvider)
	summarizer := memory.NewSummarizer(llmService)
	memoryManager := memory.NewManager(
		redisClient,
		summarizer,
		cfg.MaxContextTokens,
		cfg.ShortMemoryTokens,
		cfg.SessionTTL,
	)

	sessionRepo := session.NewRepository(redisClient, pgDB, cfg.SessionTTL)
	sessionService := session.NewService(sessionRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 6. Launch Redis Queue Summarization Worker
	summarizationWorker := worker.NewSummarizationWorker(redisClient, memoryManager)
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
				runMaintenanceTasks(ctx, log, sessionService)
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

func runMaintenanceTasks(ctx context.Context, log *slog.Logger, sessionService *session.Service) {
	log.Debug("Running background maintenance tasks...")

	if sessionService != nil {
		count, err := sessionService.PruneStaleSessions(ctx, 24*time.Hour)
		if err != nil {
			log.Error("Worker failed to prune stale sessions in PostgreSQL", "error", err)
		} else if count > 0 {
			log.Info("Worker marked stale sessions as closed", "count", count)
		}
	}
}
