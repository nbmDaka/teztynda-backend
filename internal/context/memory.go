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

// Message represents a single conversational turn
type Message struct {
	Role      MessageRole `json:"role"`
	Content   string      `json:"content"`
	Tokens    int         `json:"tokens"`
	CreatedAt time.Time   `json:"created_at"`
}

// CurrentTurn represents Level 1: Active in-flight turn before finalization
type CurrentTurn struct {
	Speaker   string    `json:"speaker"`
	Text      string    `json:"text"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SessionContext represents the 3-Level Memory System for a session
// Level 1: Current Turn (active in-flight turn)
// Level 2: Short Memory (recent conversation turns, ~1000-1500 tokens)
// Level 3: Long Memory (compacted conversation summary)
type SessionContext struct {
	SessionID   string       `json:"session_id"`
	CurrentTurn *CurrentTurn `json:"current_turn,omitempty"` // Level 1: In-flight active turn
	ShortMemory []Message    `json:"short_memory"`          // Level 2: Recent turns (~1000-1500 tokens)
	LongMemory  string       `json:"long_memory"`           // Level 3: Summary of older conversation
	TotalTokens int          `json:"total_tokens"`          // Cached sum of tokens across short & long memory
	UpdatedAt   time.Time    `json:"updated_at"`
}

// EstimateTokens provides a token count heuristic (~4 characters per token)
func EstimateTokens(text string) int {
	clean := strings.TrimSpace(text)
	if clean == "" {
		return 0
	}
	tokens := len(clean) / 4
	if tokens < 1 {
		tokens = 1
	}
	return tokens
}

// RecalculateTokens recalculates total tokens across long and short memory
func (sc *SessionContext) RecalculateTokens() {
	total := 0
	if sc.LongMemory != "" {
		total += EstimateTokens(sc.LongMemory)
	}
	for i := range sc.ShortMemory {
		if sc.ShortMemory[i].Tokens == 0 {
			sc.ShortMemory[i].Tokens = EstimateTokens(sc.ShortMemory[i].Content)
		}
		total += sc.ShortMemory[i].Tokens
	}
	sc.TotalTokens = total
}

// FormatShortMemory returns human-readable formatting of the recent conversation
func (sc *SessionContext) FormatShortMemory() string {
	var sb strings.Builder
	for _, m := range sc.ShortMemory {
		roleName := strings.Title(string(m.Role))
		sb.WriteString(fmt.Sprintf("%s: %s\n", roleName, m.Content))
	}
	return sb.String()
}
