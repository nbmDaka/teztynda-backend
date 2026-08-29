package memory

import (
	"fmt"
	"strings"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/events"
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
// Level 1: Current Turn (active in-flight turn, buffered in memory to prevent Redis write storms)
// Level 2: Short Memory (recent conversation turns, ~1000-1500 tokens)
// Level 3: Long Memory (compacted conversation summary)
type SessionContext struct {
	SessionID      string       `json:"session_id"`
	SummaryVersion int64        `json:"summary_version"`        // Optimistic version counter to prevent stale summaries
	CurrentTurn    *CurrentTurn `json:"current_turn,omitempty"` // Level 1: In-flight active turn
	ShortMemory    []Message    `json:"short_memory"`           // Level 2: Recent turns (~1000-1500 tokens)
	LongMemory     string       `json:"long_memory"`            // Level 3: Summary of older conversation
	TotalTokens    int          `json:"total_tokens"`           // Cached sum of tokens across short & long memory
	UpdatedAt      time.Time    `json:"updated_at"`
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

func formatRole(role MessageRole) string {
	switch role {
	case RoleUser:
		return "User"
	case RoleInterviewer:
		return "Interviewer"
	case RoleCandidate:
		return "Candidate"
	case RoleAssistant:
		return "Assistant"
	case RoleSystem:
		return "System"
	default:
		if len(role) > 0 {
			return strings.ToUpper(string(role[:1])) + string(role[1:])
		}
		return string(role)
	}
}

// FormatShortMemory returns human-readable formatting of the recent conversation
func (sc *SessionContext) FormatShortMemory() string {
	var sb strings.Builder
	for _, m := range sc.ShortMemory {
		sb.WriteString(fmt.Sprintf("%s: %s\n", formatRole(m.Role), m.Content))
	}
	return sb.String()
}

// BuildChatMessages constructs role-based ChatMessages for the LLM
func (sc *SessionContext) BuildChatMessages(instruction string) []events.ChatMessage {
	if instruction == "" {
		instruction = "Generate the best possible answer."
	}

	var chatMessages []events.ChatMessage

	// 1. System Prompt with Long-term memory
	var sysContent strings.Builder
	sysContent.WriteString("You are an expert AI realtime copilot and assistant.\n")
	if sc.LongMemory != "" {
		sysContent.WriteString("\n=== Long-Term Conversation Summary ===\n")
		sysContent.WriteString(sc.LongMemory)
		sysContent.WriteString("\n")
	}
	chatMessages = append(chatMessages, events.ChatMessage{
		Role:    "system",
		Content: sysContent.String(),
	})

	// 2. Short Memory Turns
	for _, msg := range sc.ShortMemory {
		role := "user"
		if msg.Role == RoleAssistant {
			role = "assistant"
		}
		content := fmt.Sprintf("[%s]: %s", formatRole(msg.Role), msg.Content)
		chatMessages = append(chatMessages, events.ChatMessage{
			Role:    role,
			Content: content,
		})
	}

	// 3. Current active in-flight turn if present
	if sc.CurrentTurn != nil && sc.CurrentTurn.Text != "" {
		speaker := sc.CurrentTurn.Speaker
		if len(speaker) > 0 {
			speaker = strings.ToUpper(speaker[:1]) + speaker[1:]
		}
		chatMessages = append(chatMessages, events.ChatMessage{
			Role:    "user",
			Content: fmt.Sprintf("[%s (currently speaking)]: %s", speaker, sc.CurrentTurn.Text),
		})
	}

	// 4. Final user prompt
	chatMessages = append(chatMessages, events.ChatMessage{
		Role:    "user",
		Content: instruction,
	})

	return chatMessages
}
