package context

import (
	"fmt"
	"strings"
	"time"
)

type MessageRole string

const (
	RoleUser        MessageRole = "user"
	RoleInterviewer MessageRole = "interviewer"
	RoleCandidate   MessageRole = "candidate"
	RoleAssistant   MessageRole = "assistant"
	RoleSystem      MessageRole = "system"
)

type Message struct {
	Role      MessageRole `json:"role"`
	Content   string      `json:"content"`
	Tokens    int         `json:"tokens"`
	Timestamp time.Time   `json:"timestamp"`
}

// SessionContext represents the complete dual-memory context of a session
type SessionContext struct {
	SessionID   string    `json:"session_id"`
	Summary     string    `json:"summary"`      // Long-term memory (compacted)
	Messages    []Message `json:"messages"`     // Short-term memory (recent raw turns)
	TotalTokens int       `json:"total_tokens"` // Estimated sum of tokens in summary + messages
	UpdatedAt   time.Time `json:"updated_at"`
}

// EstimateTokens calculates an approximate token count based on standard GPT tokenization heuristics (~4 chars/token)
func EstimateTokens(text string) int {
	clean := strings.TrimSpace(text)
	if clean == "" {
		return 0
	}
	// Heuristic: average 4 characters per token in English / multilingual text, minimum 1
	tokens := len(clean) / 4
	if tokens < 1 {
		tokens = 1
	}
	return tokens
}

// RecalculateTokens updates the TotalTokens count of the context
func (sc *SessionContext) RecalculateTokens() {
	total := 0
	if sc.Summary != "" {
		total += EstimateTokens(sc.Summary)
	}
	for i := range sc.Messages {
		if sc.Messages[i].Tokens == 0 {
			sc.Messages[i].Tokens = EstimateTokens(sc.Messages[i].Content)
		}
		total += sc.Messages[i].Tokens
	}
	sc.TotalTokens = total
}

// FormatConversation returns a human-readable transcription of recent messages
func (sc *SessionContext) FormatConversation() string {
	var sb strings.Builder
	for _, m := range sc.Messages {
		sb.WriteString(fmt.Sprintf("%s: %s\n", strings.Title(string(m.Role)), m.Content))
	}
	return sb.String()
}
