package stt

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

var samplePhrases = []string{
	"Tell me about your experience with Go and high-concurrency systems.",
	"I built a real-time backend pipeline handling WebSockets and Redis.",
	"How do you approach context management and memory summarization?",
	"We use a 3-level memory system with sliding windows and background worker summarization.",
	"What is your strategy for scaling WebSockets to ten thousand connections?",
}

type FakeSTTProvider struct {
	sessionID      string
	transcriptChan chan events.TranscriptEvent
	seq            atomic.Int64
	chunkCount     int
	phraseIndex    int
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	closeOnce      sync.Once
	closed         bool
}

func NewFakeSTTProvider() *FakeSTTProvider {
	return &FakeSTTProvider{
		transcriptChan: make(chan events.TranscriptEvent, 100),
	}
}

func (f *FakeSTTProvider) StartSession(ctx context.Context, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sessionID = sessionID
	f.ctx, f.cancel = context.WithCancel(ctx)
	f.chunkCount = 0
	f.phraseIndex = 0
	f.seq.Store(0)
	return nil
}

// SendAudio receives audio chunks and simulates streaming STT progress
func (f *FakeSTTProvider) SendAudio(chunk []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}

	metrics.Default.IncAudioChunks()
	f.chunkCount++

	// Every 2 audio chunks, emit a partial or final transcription step
	if f.chunkCount%2 != 0 {
		return nil
	}

	targetPhrase := samplePhrases[f.phraseIndex%len(samplePhrases)]
	words := strings.Split(targetPhrase, " ")

	step := (f.chunkCount / 2) % (len(words) + 1)
	if step == 0 {
		step = 1
	}

	metrics.Default.IncTranscriptEvents()

	if step < len(words) {
		partialText := strings.Join(words[:step], " ")
		select {
		case f.transcriptChan <- events.TranscriptEvent{
			Sequence:  f.seq.Add(1),
			SessionID: f.sessionID,
			Text:      partialText,
			IsFinal:   false,
			CreatedAt: time.Now().UTC(),
		}:
		default:
		}
	} else {
		select {
		case f.transcriptChan <- events.TranscriptEvent{
			Sequence:  f.seq.Add(1),
			SessionID: f.sessionID,
			Text:      targetPhrase,
			IsFinal:   true,
			CreatedAt: time.Now().UTC(),
		}:
		default:
		}
		f.phraseIndex++
		f.chunkCount = 0
	}

	return nil
}

func (f *FakeSTTProvider) TranscriptEvents() <-chan events.TranscriptEvent {
	return f.transcriptChan
}

func (f *FakeSTTProvider) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		if f.cancel != nil {
			f.cancel()
		}
		f.mu.Unlock()

		close(f.transcriptChan)
	})
	return nil
}
