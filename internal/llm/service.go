package llm

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

type Service interface {
	GenerateAnswer(ctx context.Context, chatMessages []events.ChatMessage) (string, error)
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

func (s *service) GenerateAnswer(ctx context.Context, chatMessages []events.ChatMessage) (string, error) {
	start := time.Now()
	resp, err := s.provider.Generate(ctx, chatMessages)
	if err != nil {
		slog.Error("LLM answer generation failed", "error", err, "duration", time.Since(start))
		return "", err
	}
	slog.Debug("LLM answer generation successful", "duration", time.Since(start))
	return resp, nil
}

func (s *service) GenerateSummary(ctx context.Context, existingSummary, conversationText string) (string, error) {
	systemMsg := events.ChatMessage{
		Role:    "system",
		Content: "You are an AI real-time conversation summarizer. Your task is to synthesize the existing summary and new conversation turns into a unified, concise long-term summary. Preserve key facts, technical stack mentions, architectural decisions, and candidate achievements. Keep the summary under 200 words.",
	}

	userMsg := events.ChatMessage{
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
	resp, err := s.provider.Generate(ctx, []events.ChatMessage{systemMsg, userMsg})
	if err != nil {
		slog.Error("LLM summarization failed", "error", err, "duration", time.Since(start))
		return "", err
	}
	metrics.Default.RecordLLMLatency(time.Since(start))
	return resp, nil
}
