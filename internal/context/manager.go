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
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

type Manager interface {
	AddTranscript(ctx context.Context, sessionID, speaker, text string) error
	AddMessage(ctx context.Context, sessionID string, msg Message) error
	GetContext(ctx context.Context, sessionID string) (*SessionContext, error)
	CreateSummary(ctx context.Context, sessionID string) error
	BuildPrompt(sCtx *SessionContext, instruction string) string
}

type manager struct {
	redisClient       *storage.RedisClient
	summarizer        Summarizer
	maxContextTokens  int
	shortMemoryTokens int
	ttl               time.Duration
	// Local in-memory fallback cache (used when Redis is not configured or in tests)
	localMu    sync.RWMutex
	localStore map[string]*SessionContext
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
		shortMemoryTokens = 1000
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
			slog.Warn("Redis error when fetching context, checking local store", "error", err, "session_id", sessionID)
		}
	}

	// Fallback to local memory store
	m.localMu.RLock()
	defer m.localMu.RUnlock()
	if sCtx, exists := m.localStore[sessionID]; exists {
		// return a shallow copy
		copyCtx := *sCtx
		return &copyCtx, nil
	}

	// Initial empty context
	return &SessionContext{
		SessionID: sessionID,
		Messages:  []Message{},
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func (m *manager) saveContext(ctx context.Context, sCtx *SessionContext) error {
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

func (m *manager) AddTranscript(ctx context.Context, sessionID, speaker, text string) error {
	role := RoleInterviewer
	if speaker == "candidate" || speaker == "user" {
		role = RoleCandidate
	}

	msg := Message{
		Role:      role,
		Content:   text,
		Tokens:    EstimateTokens(text),
		Timestamp: time.Now().UTC(),
	}

	return m.AddMessage(ctx, sessionID, msg)
}

func (m *manager) AddMessage(ctx context.Context, sessionID string, msg Message) error {
	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return err
	}

	if msg.Tokens == 0 {
		msg.Tokens = EstimateTokens(msg.Content)
	}

	sCtx.Messages = append(sCtx.Messages, msg)
	sCtx.RecalculateTokens()

	if err := m.saveContext(ctx, sCtx); err != nil {
		return err
	}

	// Check if token limit exceeded to trigger background summarization
	if sCtx.TotalTokens >= m.maxContextTokens {
		go func(sessID string) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := m.CreateSummary(bgCtx, sessID); err != nil {
				slog.Error("Auto-summarization failed", "session_id", sessID, "error", err)
			}
		}(sessionID)
	}

	return nil
}

func (m *manager) CreateSummary(ctx context.Context, sessionID string) error {
	// 1. Acquire distributed lock
	if m.redisClient != nil {
		locked, err := m.redisClient.AcquireLock(ctx, m.lockKey(sessionID), 15*time.Second)
		if err != nil || !locked {
			slog.Debug("Summarization lock not acquired, skipping concurrent run", "session_id", sessionID)
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

	if len(sCtx.Messages) <= 1 {
		return nil
	}

	// 2. Determine split boundary: keep the tail of messages up to shortMemoryTokens
	accumulatedRecentTokens := 0
	splitIndex := len(sCtx.Messages) - 1

	for i := len(sCtx.Messages) - 1; i >= 0; i-- {
		accumulatedRecentTokens += sCtx.Messages[i].Tokens
		if accumulatedRecentTokens >= m.shortMemoryTokens {
			splitIndex = i
			break
		}
	}

	if splitIndex <= 0 {
		splitIndex = len(sCtx.Messages) / 2
	}

	messagesToSummarize := sCtx.Messages[:splitIndex]
	recentMessages := sCtx.Messages[splitIndex:]

	if len(messagesToSummarize) == 0 {
		return nil
	}

	slog.Info("Running context summarization",
		"session_id", sessionID,
		"summarizing_count", len(messagesToSummarize),
		"preserved_count", len(recentMessages),
		"prev_tokens", sCtx.TotalTokens,
	)

	// 3. Generate summary via LLM
	newSummary, err := m.summarizer.Summarize(ctx, sCtx.Summary, messagesToSummarize)
	if err != nil {
		return fmt.Errorf("summarizer error: %w", err)
	}

	// 4. Update and persist context
	sCtx.Summary = newSummary
	sCtx.Messages = recentMessages
	sCtx.RecalculateTokens()

	slog.Info("Summarization completed",
		"session_id", sessionID,
		"new_tokens", sCtx.TotalTokens,
	)

	return m.saveContext(ctx, sCtx)
}

func (m *manager) BuildPrompt(sCtx *SessionContext, instruction string) string {
	if instruction == "" {
		instruction = "Generate the best, most concise and impactful answer or suggestion for the candidate based on the current interview flow."
	}

	var sb strings.Builder
	sb.WriteString("System:\nYou are an expert real-time AI interview copilot.\n\n")

	if sCtx.Summary != "" {
		sb.WriteString("=== Long-Term Memory Summary ===\n")
		sb.WriteString(sCtx.Summary)
		sb.WriteString("\n\n")
	}

	if len(sCtx.Messages) > 0 {
		sb.WriteString("=== Recent Conversation Turns ===\n")
		sb.WriteString(sCtx.FormatConversation())
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("User Instruction:\n%s\n", instruction))
	return sb.String()
}
