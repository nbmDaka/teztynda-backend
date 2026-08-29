package stt_test

import (
	"context"
	"testing"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/stt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeSTTProvider_Streaming(t *testing.T) {
	provider := stt.NewFakeSTTProvider()
	ctx := context.Background()
	sessionID := "test-stt-session"

	err := provider.StartSession(ctx, sessionID)
	require.NoError(t, err)

	ch := provider.TranscriptEvents()

	// Send dummy audio chunks
	for i := 0; i < 6; i++ {
		err := provider.SendAudio(ctx, []byte{0x01, 0x02, 0x03, 0x04})
		require.NoError(t, err)
	}

	// Should receive at least one transcript event
	select {
	case event, ok := <-ch:
		require.True(t, ok)
		assert.Equal(t, sessionID, event.SessionID)
		assert.NotEmpty(t, event.Text)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for transcript event")
	}

	err = provider.Close()
	require.NoError(t, err)
}
