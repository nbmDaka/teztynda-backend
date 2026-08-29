package stt

import (
	"context"
)

// STTProvider defines the contract for real-time speech-to-text providers
type STTProvider interface {
	// StartSession initializes the streaming session for a given sessionID
	StartSession(ctx context.Context, sessionID string) error

	// SendAudio streams raw PCM/encoded audio bytes to the STT provider
	SendAudio(ctx context.Context, chunk []byte) error

	// TranscriptEvents returns a read-only channel of transcription events (both partial and final)
	TranscriptEvents() <-chan TranscriptEvent

	// Close terminates the STT streaming session and cleans up resources
	Close() error
}

// ProviderFactory is a factory function type for instantiating STT providers per connection
type ProviderFactory func() STTProvider
