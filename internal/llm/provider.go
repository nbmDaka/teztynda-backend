package llm

import (
	"context"
)

// StreamChunk represents a streamed delta token from the LLM
type StreamChunk struct {
	Content string
	IsFinal bool
	Error   error
}

// LLMProvider defines the contract for role-based Large Language Model completions
type LLMProvider interface {
	Generate(ctx context.Context, messages []ChatMessage) (string, error)
	StreamGenerate(ctx context.Context, messages []ChatMessage) (<-chan StreamChunk, error)
}
