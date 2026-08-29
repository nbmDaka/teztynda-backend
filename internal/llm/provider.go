package llm

import (
	"context"
)

// LLMProvider defines the contract for Large Language Model text generation
type LLMProvider interface {
	Generate(ctx context.Context, prompt string) (string, error)
}
