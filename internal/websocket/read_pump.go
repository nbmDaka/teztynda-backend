package websocket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/events"
)

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
					slog.Info("WebSocket read closed", "session_id", c.sessionID, "error", err)
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
		// Audio Pipeline: WebSocket -> Audio Chunk -> STT Service (direct streaming, no disk storage)
		c.handleAudioChunk(inMsg.Data)

	case events.TypeGenerateAnswer:
		// Command Pipeline: Spawns non-blocking goroutine so readPump is never blocked
		go c.handleGenerateAnswer(inMsg.Prompt)

	case events.TypeClientPing:
		c.sendMessage(events.OutboundMessage{
			Type:      events.TypeServerPong,
			Timestamp: time.Now().UnixMilli(),
		})

	default:
		slog.Debug("Unknown inbound message type", "type", inMsg.Type, "session_id", c.sessionID)
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

	// Stream audio bytes directly to STT provider via STT service
	if err := c.sttService.ProcessAudio(c.sttProvider, audioBytes); err != nil {
		slog.Error("Failed to stream audio to STT service", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("stt streaming error"))
	}
}

func (c *Connection) handleGenerateAnswer(customPrompt string) {
	slog.Info("Handling generate_answer command", "session_id", c.sessionID)

	sCtx, err := c.contextManager.GetContext(c.ctx, c.sessionID)
	if err != nil {
		slog.Error("Failed to retrieve context for LLM generation", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("failed to retrieve conversation context"))
		return
	}

	fullPrompt := c.contextManager.BuildPrompt(sCtx, customPrompt)

	genCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	answerText, err := c.llmService.GenerateAnswer(genCtx, fullPrompt)
	if err != nil {
		slog.Error("LLM generation error", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("llm generation failed"))
		return
	}

	// 1. Send answer to client via writePump
	c.sendMessage(events.NewAnswerMessage(c.sessionID, answerText))

	// 2. Append assistant answer to Context Manager (Short Memory)
	_ = c.contextManager.AddMessage(c.ctx, c.sessionID, ctxpkg.Message{
		Role:      ctxpkg.RoleAssistant,
		Content:   answerText,
		CreatedAt: time.Now().UTC(),
	})

	// 3. Asynchronously record generated answer to PostgreSQL
	go func() {
		bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer bgCancel()
		_ = c.sessionService.RecordAnswer(bgCtx, c.sessionID, customPrompt, answerText)
	}()
}
