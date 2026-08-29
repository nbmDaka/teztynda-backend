package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024 // 512 KB
)

// Connection encapsulates a client's realtime WebSocket connection session
type Connection struct {
	sessionID      string
	userID         string
	ws             *websocket.Conn
	send           chan []byte
	sttService     stt.Service
	sttProvider    stt.STTProvider
	llmService     llm.Service
	contextManager ctxpkg.Manager
	sessionService session.Service
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	closeOnce      sync.Once
}

func NewConnection(
	sessionID, userID string,
	ws *websocket.Conn,
	sttService stt.Service,
	sttProvider stt.STTProvider,
	llmService llm.Service,
	contextManager ctxpkg.Manager,
	sessionService session.Service,
) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &Connection{
		sessionID:      sessionID,
		userID:         userID,
		ws:             ws,
		send:           make(chan []byte, 256),
		sttService:     sttService,
		sttProvider:    sttProvider,
		llmService:     llmService,
		contextManager: contextManager,
		sessionService: sessionService,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start launches the read, write, and STT transcript consumer pumps
func (c *Connection) Start() {
	// Send initial session started notification
	c.sendMessage(events.NewSessionStartedMessage(c.sessionID))

	c.wg.Add(3)
	go c.readPump()
	go c.writePump()
	go c.transcriptPump()

	c.wg.Wait()
	c.cleanup()
}

func (c *Connection) sendMessage(msg events.OutboundMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Failed to marshal outbound message", "error", err)
		return
	}

	select {
	case c.send <- data:
	default:
		slog.Warn("Send buffer full, dropping message frame", "session_id", c.sessionID)
	}
}

// transcriptPump receives STT streaming results and broadcasts them to the client & context manager
func (c *Connection) transcriptPump() {
	defer func() {
		c.cancel()
		c.wg.Done()
	}()

	transcripts := c.sttProvider.TranscriptEvents()

	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-transcripts:
			if !ok {
				return
			}

			// Broadcast transcript event to client (both partial and final)
			c.sendMessage(events.NewTranscriptMessage(c.sessionID, event.Text, event.IsFinal, event.CreatedAt))

			if event.IsFinal && event.Text != "" {
				// Commit to Context Manager (Short Memory)
				if err := c.contextManager.AddTranscript(c.ctx, c.sessionID, "interviewer", event.Text); err != nil {
					slog.Error("Failed to add transcript to context", "session_id", c.sessionID, "error", err)
				}

				// Asynchronous persistence to PostgreSQL (permanent history)
				go func(txt string) {
					bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer bgCancel()
					_ = c.sessionService.RecordTranscript(bgCtx, c.sessionID, "interviewer", txt, true)
				}(event.Text)
			} else if !event.IsFinal && event.Text != "" {
				// Update in-flight active turn in memory (Level 1)
				_ = c.contextManager.UpdateCurrentTurn(c.ctx, c.sessionID, "interviewer", event.Text)
			}
		}
	}
}

func (c *Connection) cleanup() {
	c.closeOnce.Do(func() {
		slog.Info("Cleaning up session connection", "session_id", c.sessionID)
		_ = c.sttService.CloseProvider(c.sttProvider)

		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.sessionService.CloseSession(bgCtx, c.sessionID)
	})
}
