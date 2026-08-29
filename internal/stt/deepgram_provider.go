package stt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbmDaka/teztynda-backend/internal/events"
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
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	closeOnce      sync.Once
	closed         bool
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

	go d.readLoop()

	return nil
}

func (d *DeepgramProvider) readLoop() {
	defer func() {
		_ = d.Close()
	}()

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			_, message, err := d.conn.ReadMessage()
			if err != nil {
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
						SessionID: d.sessionID,
						Text:      transcript,
						IsFinal:   isFinal,
						CreatedAt: time.Now().UTC(),
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

func (d *DeepgramProvider) SendAudio(chunk []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed || d.conn == nil {
		return fmt.Errorf("deepgram connection closed")
	}

	return d.conn.WriteMessage(websocket.BinaryMessage, chunk)
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

		close(d.transcriptChan)
	})
	return nil
}
