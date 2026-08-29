package llm

import (
	"context"
	"strings"
	"time"
)

type FakeLLMProvider struct {
	responseDelay time.Duration
}

func NewFakeLLMProvider(delay time.Duration) *FakeLLMProvider {
	if delay == 0 {
		delay = 100 * time.Millisecond
	}
	return &FakeLLMProvider{responseDelay: delay}
}

func (f *FakeLLMProvider) Generate(ctx context.Context, prompt string) (string, error) {
	select {
	case <-time.After(f.responseDelay):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	lowerPrompt := strings.ToLower(prompt)

	if strings.Contains(lowerPrompt, "summarize") || strings.Contains(lowerPrompt, "summary") {
		return "Summary of conversation: The candidate demonstrated in-depth knowledge of Go concurrency, modular monolith architecture, WebSocket streaming pipelines, and Redis caching. Discussed high-load scaling to 10k connections.", nil
	}

	if strings.Contains(lowerPrompt, "concurrency") || strings.Contains(lowerPrompt, "goroutine") {
		return "For high-concurrency real-time systems in Go, decoupling read and write pumps for each WebSocket connection is crucial. Use buffered channels, sync.Mutex/sync.RWMutex, atomic operations, and context cancellation to prevent goroutine leaks and race conditions.", nil
	}

	if strings.Contains(lowerPrompt, "scaling") || strings.Contains(lowerPrompt, "10000") {
		return "To scale to 10,000+ concurrent WebSocket connections: 1) Tune OS file descriptors (ulimit -n 65535), 2) Minimize per-connection buffer allocations with sync.Pool, 3) Use Redis Pub/Sub for cross-node messaging, and 4) Put an L4/L7 load balancer like Nginx or AWS ALB in front.", nil
	}

	return "I built a production-ready real-time AI assistant backend in Go using Clean Architecture, streaming audio over WebSockets to STT, maintaining dual-memory context in Redis, and auto-summarizing history before generating LLM recommendations.", nil
}
