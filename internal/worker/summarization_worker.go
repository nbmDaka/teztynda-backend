package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

type SummarizationWorker struct {
	redisClient   *storage.RedisClient
	memoryManager *memory.Manager
}

func NewSummarizationWorker(redisClient *storage.RedisClient, memoryManager *memory.Manager) *SummarizationWorker {
	return &SummarizationWorker{
		redisClient:   redisClient,
		memoryManager: memoryManager,
	}
}

// Run continuously pops and processes summarization tasks from Redis queue
func (w *SummarizationWorker) Run(ctx context.Context) {
	slog.Info("Summarization queue worker started")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Summarization queue worker stopping")
			return
		default:
			if w.redisClient == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			task, err := w.redisClient.PopSummarizationTask(ctx, 2*time.Second)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("Failed to pop task from Redis summarization queue", "error", err)
				}
				continue
			}

			if task == nil {
				continue // timeout, queue empty
			}

			slog.Info("Processing summarization task from queue",
				"session_id", task.SessionID,
				"queued_at", task.TriggeredAt,
			)

			// Derived from parent worker context so graceful shutdown cancels in-flight jobs
			taskCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := w.memoryManager.CreateSummary(taskCtx, task.SessionID); err != nil {
				slog.Error("Background summarization worker error", "session_id", task.SessionID, "error", err)
			} else {
				slog.Info("Background summarization worker finished successfully", "session_id", task.SessionID)
			}
			cancel()
		}
	}
}
