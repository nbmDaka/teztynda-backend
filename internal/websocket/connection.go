package websocket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	sttProvider    stt.STTProvider
	llmProvider    llm.LLMProvider
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
	sttProvider stt.STTProvider,
	llmProvider llm.LLMProvider,
	contextManager ctxpkg.Manager,
	sessionService session.Service,
) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &Connection{
		sessionID:      sessionID,
		userID:         userID,
		ws:             ws,
		send:           make(chan []byte, 256),
		sttProvider:    sttProvider,
		llmProvider:    llmProvider,
		contextManager: contextManager,
		sessionService: sessionService,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start launches the read, write, and STT transcript consumer goroutines
func (c *Connection) Start() {
	// Send initial session connection notification
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
		slog.Warn("Send buffer full, dropping message", "session_id", c.sessionID)
	}
}

// readPump pumps messages from the websocket connection to the server
func (c *Connection) readPump() {
	defer func() {
		c.cancel()
		c.wg.Done()
	}()

	c.ws.SetReadLimit(maxMessageSize)
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			_, message, err := c.ws.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					slog.Info("WebSocket closed", "session_id", c.sessionID, "error", err)
				}
				return
			}

			c.handleClientMessage(message)
		}
	}
}

func (c *Connection) handleClientMessage(raw []byte) {
	var inMsg events.InboundMessage
	if err := json.Unmarshal(raw, &inMsg); err != nil {
		c.sendMessage(events.NewErrorMessage("invalid json payload"))
		return
	}

	switch inMsg.Type {
	case events.TypeAudioChunk:
		c.handleAudioChunk(inMsg.Data)

	case events.TypeGenerateAnswer:
		// Execute LLM answer generation in a worker goroutine to keep readPump responsive
		go c.handleGenerateAnswer(inMsg.Prompt)

	case events.TypeClientPing:
		c.sendMessage(events.OutboundMessage{
			Type:      events.TypeServerPong,
			Timestamp: time.Now().UnixMilli(),
		})

	default:
		slog.Debug("Unknown message type received", "type", inMsg.Type, "session_id", c.sessionID)
	}
}

func (c *Connection) handleAudioChunk(base64Data string) {
	if base64Data == "" {
		return
	}

	audioBytes, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		c.sendMessage(events.NewErrorMessage("failed to decode base64 audio"))
		return
	}

	if err := c.sttProvider.SendAudio(audioBytes); err != nil {
		slog.Error("Failed to pass audio to STT provider", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("stt streaming error"))
	}
}

func (c *Connection) handleGenerateAnswer(customPrompt string) {
	slog.Info("Generating answer for session", "session_id", c.sessionID)

	sCtx, err := c.contextManager.GetContext(c.ctx, c.sessionID)
	if err != nil {
		slog.Error("Failed to retrieve session context", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("failed to get context"))
		return
	}

	fullPrompt := c.contextManager.BuildPrompt(sCtx, customPrompt)

	genCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	answerText, err := c.llmProvider.Generate(genCtx, fullPrompt)
	if err != nil {
		slog.Error("LLM generation error", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("llm generation failed"))
		return
	}

	// 1. Send answer to client via writePump
	c.sendMessage(events.NewAnswerMessage(c.sessionID, answerText))

	// 2. Add answer to context memory
	_ = c.contextManager.AddMessage(c.ctx, c.sessionID, ctxpkg.Message{
		Role:      ctxpkg.RoleAssistant,
		Content:   answerText,
		Timestamp: time.Now().UTC(),
	})

	// 3. Persist answer to PostgreSQL asynchronously
	go func() {
		bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer bgCancel()
		_ = c.sessionService.RecordAnswer(bgCtx, c.sessionID, customPrompt, answerText)
	}()
}

// transcriptPump receives STT streaming results and broadcasts them to the client & context manager
func (c *Connection) transcriptPump() {
	defer func() {
		c.cancel()
		c.wg.Done()
	}()

	transcripts := c.sttProvider.ReceiveTranscript()

	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-transcripts:
			if !ok {
				return
			}

			// Broadcast transcript event to client (both partial and final)
			c.sendMessage(events.NewTranscriptMessage(c.sessionID, event.Text, event.IsFinal, event.Timestamp))

			// If it's a final transcript, save it into the conversation context and Postgres
			if event.IsFinal && event.Text != "" {
				if err := c.contextManager.AddTranscript(c.ctx, c.sessionID, "interviewer", event.Text); err != nil {
					slog.Error("Failed to append transcript to context", "session_id", c.sessionID, "error", err)
				}

				// Asynchronous persistence to PostgreSQL
				go func(txt string) {
					bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer bgCancel()
					_ = c.sessionService.RecordTranscript(bgCtx, c.sessionID, "interviewer", txt, true)
				}(event.Text)
			}
		}
	}
}

// writePump pumps messages from the hub to the websocket connection
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.ws.Close()
		c.cancel()
		c.wg.Done()
	}()

	for {
		select {
		case <-c.ctx.Done():
			_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
			return

		case message, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.ws.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)

			// Drain any queued messages into the same write buffer for throughput optimization
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Connection) cleanup() {
	c.closeOnce.Do(func() {
		slog.Info("Cleaning up session connection", "session_id", c.sessionID)
		_ = c.sttProvider.Close()

		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.sessionService.CloseSession(bgCtx, c.sessionID)
	})
}
