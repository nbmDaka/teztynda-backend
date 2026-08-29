package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Event defines the generic contract for system events
type Event interface {
	Topic() string
}

// Handler is a callback for handling an event
type Handler func(ctx context.Context, event Event) error

// EventBus is an abstraction allowing future drop-in migration to NATS, Redis Streams, or Kafka
type EventBus interface {
	Publish(ctx context.Context, topic string, event Event) error
	Subscribe(topic string, handler Handler) error
	Close() error
}

const (
	TopicTranscriptEvents   = "events.transcript"
	TopicAnswerEvents       = "events.answer"
	TopicSessionLifecycle   = "events.session"
	TopicSummarizationQueue = "events.summarization"
)

type inMemoryEventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]Handler
	closed      bool
}

// NewInMemoryEventBus initializes a lightweight, in-memory thread-safe EventBus
func NewInMemoryEventBus() EventBus {
	return &inMemoryEventBus{
		subscribers: make(map[string][]Handler),
	}
}

func (b *inMemoryEventBus) Publish(ctx context.Context, topic string, event Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return fmt.Errorf("event bus is closed")
	}

	handlers, exists := b.subscribers[topic]
	if !exists || len(handlers) == 0 {
		return nil
	}

	for _, h := range handlers {
		// Run handlers asynchronously to avoid blocking the publisher
		go func(handler Handler, ev Event) {
			if err := handler(ctx, ev); err != nil {
				slog.Error("EventBus handler error", "topic", topic, "error", err)
			}
		}(h, event)
	}

	return nil
}

func (b *inMemoryEventBus) Subscribe(topic string, handler Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("event bus is closed")
	}

	b.subscribers[topic] = append(b.subscribers[topic], handler)
	return nil
}

func (b *inMemoryEventBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	b.subscribers = make(map[string][]Handler)
	return nil
}
