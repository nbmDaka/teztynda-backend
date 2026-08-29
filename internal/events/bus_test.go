package events_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBus_PublishSubscribe(t *testing.T) {
	bus := events.NewInMemoryEventBus()
	defer bus.Close()

	var receivedCount int64

	err := bus.Subscribe(events.TopicTranscriptEvents, func(ctx context.Context, ev events.Event) error {
		atomic.AddInt64(&receivedCount, 1)
		return nil
	})
	require.NoError(t, err)

	ctx := context.Background()
	event := events.TranscriptEvent{
		SessionID: "sess-1",
		Text:      "test transcript",
		IsFinal:   true,
		CreatedAt: time.Now(),
	}

	err = bus.Publish(ctx, events.TopicTranscriptEvents, event)
	require.NoError(t, err)

	// Wait for async handler
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int64(1), atomic.LoadInt64(&receivedCount))
}
