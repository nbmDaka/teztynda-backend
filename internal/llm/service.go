package llm

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

type Service struct {
	provider LLMProvider
}

func NewService(provider LLMProvider) *Service {
	return &Service{
		provider: provider,
	}
}

func (s *Service) GenerateAnswer(ctx context.Context, chatMessages []ChatMessage) (string, error) {
	start := time.Now()
	resp, err := s.provider.Generate(ctx, chatMessages)
	if err != nil {
		slog.Error("LLM answer generation failed", "error", err, "duration", time.Since(start))
		return "", fmt.Errorf("llm generate answer: %w", err)
	}
	slog.Debug("LLM answer generation successful", "duration", time.Since(start))
	return resp, nil
}

func (s *Service) StreamAnswer(ctx context.Context, chatMessages []ChatMessage) (<-chan StreamChunk, error) {
	return s.provider.StreamGenerate(ctx, chatMessages)
}

func (s *Service) GenerateSummary(ctx context.Context, existingSummary, conversationText string) (string, error) {
	systemMsg := ChatMessage{
		Role:    "system",
		Content: "You are an AI real-time conversation summarizer. Your task is to synthesize the existing summary and new conversation turns into a unified, concise long-term summary. Preserve key facts, technical stack mentions, architectural decisions, and candidate achievements. Keep the summary under 200 words.",
	}

	userMsg := ChatMessage{
		Role: "user",
		Content: fmt.Sprintf("Existing Summary:\n%s\n\nNew conversation turns:\n%s",
			func() string {
				if strings.TrimSpace(existingSummary) == "" {
					return "None (Conversation just started)"
				}
				return existingSummary
			}(),
			conversationText,
		),
	}

	start := time.Now()
	resp, err := s.provider.Generate(ctx, []ChatMessage{systemMsg, userMsg})
	if err != nil {
		slog.Error("LLM summarization failed", "error", err, "duration", time.Since(start))
		return "", fmt.Errorf("llm generate summary: %w", err)
	}
	metrics.Default.RecordLLMLatency(time.Since(start))
	return resp, nil
}
