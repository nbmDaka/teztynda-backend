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
	llmService llm.Service
}

func NewSummarizer(llmService llm.Service) Summarizer {
	return &summarizer{
		llmService: llmService,
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

	return s.llmService.GenerateSummary(ctx, existingSummary, sb.String())
}
