package llm

import (
	"context"

	"github.com/nbmDaka/teztynda-backend/internal/events"
)

// StreamChunk represents a streamed delta token from the LLM
type StreamChunk struct {
	Content string
	IsFinal bool
	Error   error
}

// LLMProvider defines the contract for role-based Large Language Model completions
type LLMProvider interface {
	Generate(ctx context.Context, messages []events.ChatMessage) (string, error)
	StreamGenerate(ctx context.Context, messages []events.ChatMessage) (<-chan StreamChunk, error)
}
