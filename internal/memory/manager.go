package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

type Manager struct {
	redisClient       *storage.RedisClient
	summarizer        Summarizer
	maxContextTokens  int
	shortMemoryTokens int
	ttl               time.Duration

	// Local buffer for Level 1 CurrentTurn to avoid Redis write storms on 100ms partial STT tokens
	turnMu     sync.RWMutex
	turnBuffer map[string]*CurrentTurn

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
) *Manager {
	if maxContextTokens <= 0 {
		maxContextTokens = 3000
	}
	if shortMemoryTokens <= 0 {
		shortMemoryTokens = 1200
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	return &Manager{
		redisClient:       redisClient,
		summarizer:        summarizer,
		maxContextTokens:  maxContextTokens,
		shortMemoryTokens: shortMemoryTokens,
		ttl:               ttl,
		turnBuffer:        make(map[string]*CurrentTurn),
		localStore:        make(map[string]*SessionContext),
	}
}

func (m *Manager) contextKey(sessionID string) string {
	return fmt.Sprintf("context:%s", sessionID)
}

func (m *Manager) lockKey(sessionID string) string {
	return fmt.Sprintf("lock:summary:%s", sessionID)
}

func (m *Manager) GetContext(ctx context.Context, sessionID string) (*SessionContext, error) {
	var sCtx *SessionContext

	if m.redisClient != nil {
		val, err := m.redisClient.Client().Get(ctx, m.contextKey(sessionID)).Result()
		if err == nil {
			var parsed SessionContext
			if err := json.Unmarshal([]byte(val), &parsed); err == nil {
				sCtx = &parsed
			}
		} else if !errors.Is(err, redis.Nil) {
			slog.Warn("Redis error when fetching context, checking local fallback store", "error", err, "session_id", sessionID)
		}
	}

	if sCtx == nil {
		m.localMu.RLock()
		if local, exists := m.localStore[sessionID]; exists {
			copied := *local
			sCtx = &copied
		}
		m.localMu.RUnlock()
	}

	if sCtx == nil {
		sCtx = &SessionContext{
			SessionID:      sessionID,
			SummaryVersion: 1,
			ShortMemory:    []Message{},
			UpdatedAt:      time.Now().UTC(),
		}
	}

	// Attach active in-memory Level 1 CurrentTurn
	m.turnMu.RLock()
	if activeTurn, exists := m.turnBuffer[sessionID]; exists {
		turnCopy := *activeTurn
		sCtx.CurrentTurn = &turnCopy
	}
	m.turnMu.RUnlock()

	return sCtx, nil
}

func (m *Manager) SaveContext(ctx context.Context, sCtx *SessionContext) error {
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

// UpdateCurrentTurn updates Level 1 active turn in local memory buffer ONLY
// Zero Redis network calls for 100ms partial STT streaming updates!
func (m *Manager) UpdateCurrentTurn(ctx context.Context, sessionID, speaker, text string) error {
	m.turnMu.Lock()
	m.turnBuffer[sessionID] = &CurrentTurn{
		Speaker:   speaker,
		Text:      text,
		UpdatedAt: time.Now().UTC(),
	}
	m.turnMu.Unlock()
	return nil
}

func (m *Manager) AddTranscript(ctx context.Context, sessionID, speaker, text string) error {
	// Clear Level 1 in-flight turn buffer upon final turn commit
	m.turnMu.Lock()
	delete(m.turnBuffer, sessionID)
	m.turnMu.Unlock()

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

func (m *Manager) AddMessage(ctx context.Context, sessionID string, msg Message) error {
	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("get context for adding message: %w", err)
	}

	// Reset Level 1 turn
	sCtx.CurrentTurn = nil

	if msg.Tokens == 0 {
		msg.Tokens = EstimateTokens(msg.Content)
	}

	sCtx.ShortMemory = append(sCtx.ShortMemory, msg)
	sCtx.RecalculateTokens()

	if err := m.SaveContext(ctx, sCtx); err != nil {
		return fmt.Errorf("save context after adding message: %w", err)
	}

	// Check if token limit exceeded to enqueue asynchronous background task
	if sCtx.TotalTokens >= m.maxContextTokens {
		if m.redisClient != nil {
			task := SummarizationTask{
				SessionID:      sessionID,
				SummaryVersion: sCtx.SummaryVersion,
				TriggeredAt:    time.Now().UTC(),
			}
			taskBytes, err := json.Marshal(task)
			if err != nil {
				slog.Error("Failed to marshal summarization task", "session_id", sessionID, "error", err)
			} else if err := m.redisClient.LPush(ctx, QueueSummarization, taskBytes); err != nil {
				slog.Error("Failed to enqueue summarization task to Redis queue", "session_id", sessionID, "error", err)
			} else {
				slog.Info("Summarization job enqueued to Redis background queue", "session_id", sessionID, "tokens", sCtx.TotalTokens)
			}
		} else {
			go func(sessID string) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_ = m.CreateSummary(bgCtx, sessID)
			}(sessionID)
		}
	}

	return nil
}

// DeleteSession evicts in-memory buffer and local cache for the specified session (Fix 2)
func (m *Manager) DeleteSession(sessionID string) {
	m.turnMu.Lock()
	delete(m.turnBuffer, sessionID)
	m.turnMu.Unlock()

	m.localMu.Lock()
	delete(m.localStore, sessionID)
	m.localMu.Unlock()
}

func (m *Manager) CreateSummary(ctx context.Context, sessionID string) error {
	// 1. Acquire distributed lock with TTL
	if m.redisClient != nil {
		locked, err := m.redisClient.AcquireLock(ctx, m.lockKey(sessionID), 25*time.Second)
		if err != nil || !locked {
			slog.Debug("Summarization lock already held, skipping duplicate worker run", "session_id", sessionID)
			return nil
		}
		defer func() {
			lockCtx, lCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer lCancel()
			_ = m.redisClient.ReleaseLock(lockCtx, m.lockKey(sessionID))
		}()
	}

	sCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("get context for summary creation: %w", err)
	}

	if len(sCtx.ShortMemory) <= 1 {
		return nil
	}

	initialVersion := sCtx.SummaryVersion
	initialMsgCount := len(sCtx.ShortMemory)

	// 2. Determine split index: keep recent turns up to shortMemoryTokens in Level 2
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
		"version", initialVersion,
	)

	// 3. Generate summary via LLM
	newSummary, err := m.summarizer.Summarize(ctx, sCtx.LongMemory, messagesToSummarize)
	if err != nil {
		return fmt.Errorf("summarizer error: %w", err)
	}

	// 4. Reload freshest context before saving to prevent race conditions with newly added turns
	freshCtx, err := m.GetContext(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to reload fresh context: %w", err)
	}

	// Optimistic concurrency check
	if freshCtx.SummaryVersion != initialVersion {
		slog.Warn("Summarization version conflict detected, skipping overwrite",
			"session_id", sessionID,
			"expected_version", initialVersion,
			"actual_version", freshCtx.SummaryVersion,
		)
		return nil
	}

	if len(freshCtx.ShortMemory) > initialMsgCount {
		// New messages were appended while LLM was summarizing; preserve them safely
		newlyAdded := freshCtx.ShortMemory[initialMsgCount:]
		recentMessages = append(recentMessages, newlyAdded...)
	}

	// 5. Update 3-level context state and increment optimistic version
	freshCtx.LongMemory = newSummary
	freshCtx.ShortMemory = recentMessages
	freshCtx.SummaryVersion = initialVersion + 1
	freshCtx.RecalculateTokens()

	slog.Info("Summarization complete and memory updated",
		"session_id", sessionID,
		"new_tokens", freshCtx.TotalTokens,
		"new_version", freshCtx.SummaryVersion,
	)

	return m.SaveContext(ctx, freshCtx)
}

func (m *Manager) BuildChatMessages(sCtx *SessionContext, instruction string) []llm.ChatMessage {
	return sCtx.BuildChatMessages(instruction)
}

func (m *Manager) BuildPrompt(sCtx *SessionContext, instruction string) string {
	chatMsgs := sCtx.BuildChatMessages(instruction)
	var sb strings.Builder
	for _, msg := range chatMsgs {
		sb.WriteString(fmt.Sprintf("[%s]: %s\n", msg.Role, msg.Content))
	}
	return sb.String()
}
