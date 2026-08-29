package context

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

type Manager interface {
	UpdateCurrentTurn(ctx context.Context, sessionID, speaker, text string) error
	AddTranscript(ctx context.Context, sessionID, speaker, text string) error
	AddMessage(ctx context.Context, sessionID string, msg Message) error
	GetContext(ctx context.Context, sessionID string) (*SessionContext, error)
	CreateSummary(ctx context.Context, sessionID string) error
	BuildPrompt(sCtx *SessionContext, instruction string) string
	SaveContext(ctx context.Context, sCtx *SessionContext) error
}

type manager struct {
	redisClient       *storage.RedisClient
	summarizer        Summarizer
	maxContextTokens  int
	shortMemoryTokens int
	ttl               time.Duration
	localMu           sync.RWMutex
	localStore        map[string]*SessionContext
}

func NewManager(
	redisClient *storage.RedisClient,
	summarizer Summarizer,
	maxContextTokens int,
	shortMemoryTokens int,
	ttl time.Duration,
) Manager {
	if maxContextTokens <= 0 {
		maxContextTokens = 3000
	}
	if shortMemoryTokens <= 0 {
		shortMemoryTokens = 1200
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	return &manager{
		redisClient:       redisClient,
		summarizer:        summarizer,
		maxContextTokens:  maxContextTokens,
		shortMemoryTokens: shortMemoryTokens,
		ttl:               ttl,
		localStore:        make(map[string]*SessionContext),
	}
}

func (m *manager) contextKey(sessionID string) string {
	return fmt.Sprintf("context:%s", sessionID)
}

func (m *manager) lockKey(sessionID string) string {
	return fmt.Sprintf("lock:summary:%s", sessionID)
}

func (m *manager) GetContext(ctx context.Context, sessionID string) (*SessionContext, error) {
	if m.redisClient != nil {
		val, err := m.redisClient.Client().Get(ctx, m.contextKey(sessionID)).Result()
		if err == nil {
			var sCtx SessionContext
			if err := json.Unmarshal([]byte(val), &sCtx); err == nil {
				return &sCtx, nil
			}
		} else if !errors.Is(err, redis.Nil) {
			slog.Warn("Redis error when fetching context, checking local fallback store", "error", err, "session_id", sessionID)
		}
	}

	// Fallback to local memory store
	m.localMu.RLock()
	defer m.localMu.RUnlock()
	if sCtx, exists := m.localStore[sessionID]; exists {
		copyCtx := *sCtx
		return &copyCtx, nil
	}

	// Initial empty 3-level context
	return &SessionContext{
		SessionID:   sessionID,
		ShortMemory: []Message{},
		UpdatedAt:   time.Now().UTC(),
	}, nil
}

func (m *manager) SaveContext(ctx context.Context, sCtx *SessionContext) error {
	sCtx.UpdatedAt = time.Now().UTC()
	sCtx.RecalculateTokens()

	if m.redisClient != nil {
		data, err := json.Marshal(sCtx)
		if err != nil {
			return fmt.Errorf("failed to marshal context: %w", err)
		}
		if err := m.redisClient.Client().Set(ctx, m.contextKey(sCtx.SessionID), data, m.ttl).Err(); err != nil {
			return fmt.Errorf("failed to save context to redis: %w", err)
		}
	}

	m.localMu.Lock()
	m.localStore[sCtx.SessionID] = sCtx
	m.localMu.Unlock()

	return nil
}

func (m *manager) UpdateCurrentTurn(ctx context.Context, sessionID, speaker, text string) error {
	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return err
	}

	sCtx.CurrentTurn = &CurrentTurn{
		Speaker:   speaker,
		Text:      text,
		UpdatedAt: time.Now().UTC(),
	}

	return m.SaveContext(ctx, sCtx)
}

func (m *manager) AddTranscript(ctx context.Context, sessionID, speaker, text string) error {
	role := RoleInterviewer
	if speaker == "candidate" || speaker == "user" {
		role = RoleCandidate
	}

	msg := Message{
		Role:      role,
		Content:   text,
		Tokens:    EstimateTokens(text),
		CreatedAt: time.Now().UTC(),
	}

	return m.AddMessage(ctx, sessionID, msg)
}

func (m *manager) AddMessage(ctx context.Context, sessionID string, msg Message) error {
	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return err
	}

	// Clear in-flight active turn once final turn is committed
	sCtx.CurrentTurn = nil

	if msg.Tokens == 0 {
		msg.Tokens = EstimateTokens(msg.Content)
	}

	sCtx.ShortMemory = append(sCtx.ShortMemory, msg)
	sCtx.RecalculateTokens()

	if err := m.SaveContext(ctx, sCtx); err != nil {
		return err
	}

	// Asynchronous trigger: enqueue task to Redis Queue when token threshold is reached
	if sCtx.TotalTokens >= m.maxContextTokens {
		if m.redisClient != nil {
			task := events.SummarizationTask{
				SessionID:   sessionID,
				TriggeredAt: time.Now().UTC(),
			}
			if err := m.redisClient.PushSummarizationTask(ctx, task); err != nil {
				slog.Error("Failed to enqueue summarization task to Redis queue", "session_id", sessionID, "error", err)
			} else {
				slog.Info("Summarization job enqueued to Redis background queue", "session_id", sessionID, "total_tokens", sCtx.TotalTokens)
			}
		} else {
			// In standalone test mode without Redis, run in background goroutine
			go func(sessID string) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_ = m.CreateSummary(bgCtx, sessID)
			}(sessionID)
		}
	}

	return nil
}

func (m *manager) CreateSummary(ctx context.Context, sessionID string) error {
	// 1. Acquire distributed lock
	if m.redisClient != nil {
		locked, err := m.redisClient.AcquireLock(ctx, m.lockKey(sessionID), 20*time.Second)
		if err != nil || !locked {
			slog.Debug("Summarization lock already held, skipping duplicate task", "session_id", sessionID)
			return nil
		}
		defer func() {
			_ = m.redisClient.ReleaseLock(context.Background(), m.lockKey(sessionID))
		}()
	}

	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return err
	}

	if len(sCtx.ShortMemory) <= 1 {
		return nil
	}

	// 2. Determine split index to keep recent ~shortMemoryTokens in short memory
	accumulatedRecentTokens := 0
	splitIndex := len(sCtx.ShortMemory) - 1

	for i := len(sCtx.ShortMemory) - 1; i >= 0; i-- {
		accumulatedRecentTokens += sCtx.ShortMemory[i].Tokens
		if accumulatedRecentTokens >= m.shortMemoryTokens {
			splitIndex = i
			break
		}
	}

	if splitIndex <= 0 {
		splitIndex = len(sCtx.ShortMemory) / 2
	}

	messagesToSummarize := sCtx.ShortMemory[:splitIndex]
	recentMessages := sCtx.ShortMemory[splitIndex:]

	if len(messagesToSummarize) == 0 {
		return nil
	}

	slog.Info("Executing background summarization",
		"session_id", sessionID,
		"compacting_count", len(messagesToSummarize),
		"preserved_count", len(recentMessages),
		"prev_tokens", sCtx.TotalTokens,
	)

	// 3. Generate summary via LLM
	newSummary, err := m.summarizer.Summarize(ctx, sCtx.LongMemory, messagesToSummarize)
	if err != nil {
		return fmt.Errorf("summarizer error: %w", err)
	}

	// 4. Update 3-level context state
	sCtx.LongMemory = newSummary
	sCtx.ShortMemory = recentMessages
	sCtx.RecalculateTokens()

	slog.Info("Summarization complete and memory updated",
		"session_id", sessionID,
		"new_tokens", sCtx.TotalTokens,
	)

	return m.SaveContext(ctx, sCtx)
}

func (m *manager) BuildPrompt(sCtx *SessionContext, instruction string) string {
	if instruction == "" {
		instruction = "Generate the best possible answer."
	}

	var sb strings.Builder
	sb.WriteString("System:\nYou are an AI realtime assistant.\n\n")

	sb.WriteString("Conversation context:\n")
	if sCtx.LongMemory != "" {
		sb.WriteString("=== Long Memory Summary ===\n")
		sb.WriteString(sCtx.LongMemory)
		sb.WriteString("\n\n")
	}

	if len(sCtx.ShortMemory) > 0 {
		sb.WriteString("=== Short Memory Turns ===\n")
		sb.WriteString(sCtx.FormatShortMemory())
		sb.WriteString("\n")
	}

	if sCtx.CurrentTurn != nil && sCtx.CurrentTurn.Text != "" {
		sb.WriteString(fmt.Sprintf("=== Current Turn ===\n%s: %s (in progress)\n\n", sCtx.CurrentTurn.Speaker, sCtx.CurrentTurn.Text))
	}

	sb.WriteString(fmt.Sprintf("User Instruction:\n%s\n", instruction))
	return sb.String()
}
