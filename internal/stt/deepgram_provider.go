package stt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

var (
	ErrConnectionClosed = errors.New("deepgram connection closed")
)

type DeepgramResponse struct {
	Type        string `json:"type"`
	IsFinal     bool   `json:"is_final"`
	SpeechFinal bool   `json:"speech_final"`
	Channel     struct {
		Alternatives []struct {
			Transcript string  `json:"transcript"`
			Confidence float64 `json:"confidence"`
		} `json:"alternatives"`
	} `json:"channel"`
}

type DeepgramProvider struct {
	apiKey         string
	sessionID      string
	conn           *websocket.Conn
	transcriptChan chan events.TranscriptEvent
	seq            atomic.Int64
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	closeOnce      sync.Once
	closed         bool
	readWg         sync.WaitGroup
}

func NewDeepgramProvider(apiKey string) *DeepgramProvider {
	return &DeepgramProvider{
		apiKey:         apiKey,
		transcriptChan: make(chan events.TranscriptEvent, 100),
	}
}

func (d *DeepgramProvider) StartSession(ctx context.Context, sessionID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.sessionID = sessionID
	d.ctx, d.cancel = context.WithCancel(ctx)

	endpoint := "wss://api.deepgram.com/v1/listen?encoding=linear16&sample_rate=16000&channels=1&interim_results=true&smart_format=true&endpointing=300"
	header := http.Header{}
	header.Set("Authorization", "Token "+d.apiKey)

	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.DialContext(d.ctx, endpoint, header)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("failed to dial deepgram (status %d): %w", resp.StatusCode, err)
		}
		return fmt.Errorf("failed to dial deepgram: %w", err)
	}
	d.conn = conn

	d.readWg.Add(1)
	go d.readLoop()

	return nil
}

func (d *DeepgramProvider) readLoop() {
	defer d.readWg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			_, message, err := d.conn.ReadMessage()
			if err != nil {
				d.mu.Lock()
				isClosed := d.closed
				d.mu.Unlock()
				if !isClosed && d.ctx.Err() == nil {
					slog.Warn("Deepgram STT read loop closed", "session_id", d.sessionID, "error", err)
					metrics.Default.IncSTTErrors()
				}
				return
			}

			var resp DeepgramResponse
			if err := json.Unmarshal(message, &resp); err != nil {
				continue
			}

			if len(resp.Channel.Alternatives) > 0 {
				transcript := resp.Channel.Alternatives[0].Transcript
				if transcript != "" {
					isFinal := resp.IsFinal || resp.SpeechFinal
					event := events.TranscriptEvent{
						Sequence:  d.seq.Add(1),
						SessionID: d.sessionID,
						Text:      transcript,
						IsFinal:   isFinal,
						CreatedAt: time.Now().UTC(),
					}

					metrics.Default.IncTranscriptEvents()

					d.mu.Lock()
					isClosed := d.closed
					d.mu.Unlock()

					if isClosed {
						return
					}

					select {
					case d.transcriptChan <- event:
					case <-d.ctx.Done():
						return
					}
				}
			}
		}
	}
}

func (d *DeepgramProvider) SendAudio(ctx context.Context, chunk []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed || d.conn == nil {
		return ErrConnectionClosed
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("send audio context canceled: %w", ctx.Err())
	default:
	}

	metrics.Default.IncAudioChunks()
	if err := d.conn.WriteMessage(websocket.BinaryMessage, chunk); err != nil {
		return fmt.Errorf("write audio to deepgram: %w", err)
	}
	return nil
}

func (d *DeepgramProvider) TranscriptEvents() <-chan events.TranscriptEvent {
	return d.transcriptChan
}

func (d *DeepgramProvider) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		if d.cancel != nil {
			d.cancel()
		}
		if d.conn != nil {
			_ = d.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			_ = d.conn.Close()
		}
		d.mu.Unlock()

		// Wait for reader goroutine to exit before closing channel to guarantee zero panics
		d.readWg.Wait()
		close(d.transcriptChan)
		slog.Debug("Deepgram STT provider closed cleanly", "session_id", d.sessionID)
	})
	return nil
}
