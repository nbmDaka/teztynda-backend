package context

import (
	"context"
	"fmt"
	"strings"

	"github.com/nbmDaka/teztynda-backend/internal/llm"
)

type Summarizer interface {
	Summarize(ctx context.Context, existingSummary string, messages []Message) (string, error)
}

type summarizer struct {
	llmProvider llm.LLMProvider
}

func NewSummarizer(provider llm.LLMProvider) Summarizer {
	return &summarizer{
		llmProvider: provider,
	}
}

func (s *summarizer) Summarize(ctx context.Context, existingSummary string, messages []Message) (string, error) {
	if len(messages) == 0 {
		return existingSummary, nil
	}

	var sb strings.Builder
	for _, m := range messages {
		sb.WriteString(fmt.Sprintf("%s: %s\n", strings.ToUpper(string(m.Role)), m.Content))
	}

	prompt := fmt.Sprintf(`You are an AI real-time conversation summarizer.

Existing Summary:
%s

New conversation turns to incorporate:
%s

Task:
Synthesize the existing summary and the new conversation turns into a unified, concise long-term summary. Preserve key facts, technical stack mentions, architectural decisions, and candidate achievements. Keep the summary under 200 words.`,
		func() string {
			if existingSummary == "" {
				return "None (Conversation just started)"
			}
			return existingSummary
		}(),
		sb.String(),
	)

	newSummary, err := s.llmProvider.Generate(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("failed to generate summary from LLM: %w", err)
	}

	return strings.TrimSpace(newSummary), nil
}
