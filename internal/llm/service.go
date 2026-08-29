package llm

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Service interface {
	GenerateAnswer(ctx context.Context, contextPrompt string) (string, error)
	GenerateSummary(ctx context.Context, existingSummary, conversationText string) (string, error)
}

type service struct {
	provider LLMProvider
}

func NewService(provider LLMProvider) Service {
	return &service{
		provider: provider,
	}
}

func (s *service) GenerateAnswer(ctx context.Context, contextPrompt string) (string, error) {
	start := time.Now()
	resp, err := s.provider.Generate(ctx, contextPrompt)
	if err != nil {
		slog.Error("LLM generation failed", "error", err, "duration", time.Since(start))
		return "", err
	}
	slog.Debug("LLM generation successful", "duration", time.Since(start))
	return resp, nil
}

func (s *service) GenerateSummary(ctx context.Context, existingSummary, conversationText string) (string, error) {
	prompt := fmt.Sprintf(`You are an AI real-time conversation summarizer.

Existing Summary:
%s

New conversation turns to incorporate:
%s

Task:
Synthesize the existing summary and the new conversation turns into a unified, concise long-term summary. Preserve key facts, technical stack mentions, architectural decisions, and candidate achievements. Keep the summary under 200 words.`,
		func() string {
			if strings.TrimSpace(existingSummary) == "" {
				return "None (Conversation just started)"
			}
			return existingSummary
		}(),
		conversationText,
	)

	return s.provider.Generate(ctx, prompt)
}
