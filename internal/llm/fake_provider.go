package llm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

type FakeLLMProvider struct {
	responseDelay time.Duration
}

func NewFakeLLMProvider(delay time.Duration) *FakeLLMProvider {
	if delay == 0 {
		delay = 50 * time.Millisecond
	}
	return &FakeLLMProvider{responseDelay: delay}
}

func (f *FakeLLMProvider) Generate(ctx context.Context, messages []ChatMessage) (string, error) {
	start := time.Now()
	metrics.Default.IncLLMRequests()

	select {
	case <-time.After(f.responseDelay):
	case <-ctx.Done():
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("fake llm generation canceled: %w", ctx.Err())
	}

	metrics.Default.RecordLLMLatency(time.Since(start))

	var combined strings.Builder
	for _, m := range messages {
		combined.WriteString(m.Content + " ")
	}
	lower := strings.ToLower(combined.String())

	if strings.Contains(lower, "summarize") || strings.Contains(lower, "summary") {
		return "Summary of conversation: The candidate demonstrated in-depth knowledge of Go concurrency, modular monolith architecture, WebSocket streaming pipelines, and Redis caching. Discussed high-load scaling to 10k connections.", nil
	}

	if strings.Contains(lower, "concurrency") || strings.Contains(lower, "goroutine") {
		return "For high-concurrency real-time systems in Go, decoupling read and write pumps for each WebSocket connection is crucial. Use buffered channels, sync.Mutex/sync.RWMutex, atomic operations, and context cancellation to prevent goroutine leaks and race conditions.", nil
	}

	if strings.Contains(lower, "scaling") || strings.Contains(lower, "10000") {
		return "To scale to 10,000+ concurrent WebSocket connections: 1) Tune OS file descriptors (ulimit -n 65535), 2) Minimize per-connection buffer allocations with sync.Pool, 3) Use Redis Pub/Sub for cross-node messaging, and 4) Put an L4/L7 load balancer like Nginx or AWS ALB in front.", nil
	}

	return "I built a production-ready real-time AI assistant backend in Go using Clean Architecture, streaming audio over WebSockets to STT, maintaining 3-level memory in Redis, and auto-summarizing history before generating LLM recommendations.", nil
}

func (f *FakeLLMProvider) StreamGenerate(ctx context.Context, messages []ChatMessage) (<-chan StreamChunk, error) {
	fullText, err := f.Generate(ctx, messages)
	if err != nil {
		return nil, err
	}

	out := make(chan StreamChunk, 32)
	words := strings.Split(fullText, " ")

	go func() {
		defer close(out)
		wordDelay := f.responseDelay / time.Duration(max(len(words), 1))
		if wordDelay > 10*time.Millisecond {
			wordDelay = 10 * time.Millisecond
		}

		for i, w := range words {
			isLast := i == len(words)-1
			token := w
			if !isLast {
				token += " "
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(wordDelay):
				select {
				case out <- StreamChunk{
					Content: token,
					IsFinal: isLast,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}
