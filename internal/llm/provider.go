package llm

import (
	"context"

	"github.com/nbmDaka/teztynda-backend/internal/events"
)

// LLMProvider defines the contract for role-based Large Language Model completions
type LLMProvider interface {
	Generate(ctx context.Context, messages []events.ChatMessage) (string, error)
}
