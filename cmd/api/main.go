package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/config"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
	"github.com/nbmDaka/teztynda-backend/internal/websocket"
	"github.com/nbmDaka/teztynda-backend/pkg/logger"
)

func main() {
	// 1. Configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Structured Logger
	log := logger.SetupLogger(cfg.AppEnv, cfg.LogLevel)
	log.Info("Starting Teztynda Realtime AI Assistant Backend",
		"env", cfg.AppEnv,
		"port", cfg.Port,
		"stt_provider", cfg.STTProvider,
		"llm_provider", cfg.LLMProvider,
	)

	// 3. PostgreSQL Initialization & Auto-migration
	var pgDB *storage.PostgresDB
	if cfg.PostgresDSN != "" {
		pgDB, err = storage.NewPostgresDB(cfg.PostgresDSN)
		if err != nil {
			log.Warn("PostgreSQL connection not established, running in degraded state", "error", err)
		} else {
			defer pgDB.Close()
			migrationCtx, mCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer mCancel()
			if err := pgDB.AutoMigrate(migrationCtx); err != nil {
				log.Error("Database schema migration failed", "error", err)
			} else {
				log.Info("PostgreSQL connected and schema migrated successfully")
			}
		}
	}

	// 4. Redis Initialization
	var redisClient *storage.RedisClient
	if cfg.RedisURL != "" {
		redisClient, err = storage.NewRedisClient(cfg.RedisURL, cfg.RedisPassword, cfg.RedisDB)
		if err != nil {
			log.Warn("Redis connection not established, falling back to local memory store", "error", err)
		} else {
			defer redisClient.Close()
			log.Info("Redis connected successfully", "addr", cfg.RedisURL)
		}
	}

	// 5. STT Provider Factory
	sttFactory := func() stt.STTProvider {
		if cfg.STTProvider == "deepgram" && cfg.DeepgramAPIKey != "" {
			return stt.NewDeepgramProvider(cfg.DeepgramAPIKey)
		}
		return stt.NewFakeSTTProvider()
	}

	// 6. LLM Provider
	var llmProvider llm.LLMProvider
	if cfg.LLMProvider == "openai" && cfg.OpenAIAPIKey != "" {
		llmProvider = llm.NewOpenAIProvider(cfg.OpenAIAPIKey, cfg.OpenAIModel)
	} else {
		llmProvider = llm.NewFakeLLMProvider(100 * time.Millisecond)
	}

	// 7. Context Management & Summarizer
	summarizer := ctxpkg.NewSummarizer(llmProvider)
	contextManager := ctxpkg.NewManager(
		redisClient,
		summarizer,
		cfg.MaxContextTokens,
		cfg.ShortMemoryTokens,
		cfg.SessionTTL,
	)

	// 8. Session Repository & Service
	sessionRepo := session.NewRepository(redisClient, pgDB, cfg.SessionTTL)
	sessionService := session.NewService(sessionRepo)

	// 9. WebSocket Handler & HTTP Router
	wsHandler := websocket.NewHandler(sttFactory, llmProvider, contextManager, sessionService)

	mux := http.NewServeMux()
	mux.Handle("/ws/realtime", wsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 10. Start Server in background goroutine
	go func() {
		log.Info("HTTP and WebSocket server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// 11. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("Server forced to shutdown", "error", err)
	}

	log.Info("Server stopped cleanly")
}
