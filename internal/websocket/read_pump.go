package websocket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
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
		// Command Pipeline: Enforce per-connection rate-limit with semaphore, execute in background goroutine
		select {
		case c.llmSemaphore <- struct{}{}:
			go func(prompt string) {
				defer func() { <-c.llmSemaphore }()
				c.handleGenerateAnswer(prompt)
			}(inMsg.Prompt)
		default:
			c.sendMessage(events.NewErrorMessage("rate limit exceeded: generation already in progress"))
		}

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

	// Security: validate base64 string length limit before decoding
	if len(base64Data) > c.maxAudioChunkBytes*2 {
		c.sendMessage(events.NewErrorMessage("audio chunk exceeds maximum allowed size"))
		return
	}

	audioBytes, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		c.sendMessage(events.NewErrorMessage("failed to decode base64 audio"))
		return
	}

	// Security: validate decoded raw audio byte length
	if len(audioBytes) > c.maxAudioChunkBytes {
		c.sendMessage(events.NewErrorMessage("decoded audio chunk exceeds maximum allowed size"))
		return
	}

	// Stream audio bytes into buffered audioQueue with backpressure handling
	select {
	case c.audioQueue <- audioBytes:
		metrics.Default.SetAudioQueueSize(int64(len(c.audioQueue)))
	default:
		slog.Warn("Audio queue full, dropping audio chunk to preserve realtime backpressure", "session_id", c.sessionID)
		metrics.Default.IncAudioDropped()
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

	// Build structured role-based messages for optimal LLM completion
	chatMessages := c.contextManager.BuildChatMessages(sCtx, customPrompt)

	genCtx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	stream, err := c.llmService.StreamAnswer(genCtx, chatMessages)
	if err != nil {
		slog.Error("LLM streaming answer initiation failed", "session_id", c.sessionID, "error", err)
		c.sendMessage(events.NewErrorMessage("llm generation failed"))
		return
	}

	var answerBuilder strings.Builder
	for chunk := range stream {
		if chunk.Error != nil {
			slog.Error("LLM stream chunk error", "session_id", c.sessionID, "error", chunk.Error)
			c.sendMessage(events.NewErrorMessage("llm streaming error"))
			return
		}

		if chunk.Content != "" {
			answerBuilder.WriteString(chunk.Content)
			c.sendMessage(events.NewAnswerChunkMessage(c.sessionID, chunk.Content, chunk.IsFinal))
		}
	}

	answerText := answerBuilder.String()
	if answerText == "" {
		slog.Warn("LLM generated empty response", "session_id", c.sessionID)
		return
	}

	// Send final answer message for ack & client state synchronization
	c.sendMessage(events.NewAnswerMessage(c.sessionID, answerText))

	// Append assistant answer to Context Manager (Short Memory)
	_ = c.contextManager.AddMessage(c.ctx, c.sessionID, ctxpkg.Message{
		Role:      ctxpkg.RoleAssistant,
		Content:   answerText,
		CreatedAt: time.Now().UTC(),
	})

	// Asynchronously record generated answer to PostgreSQL
	go func(ans string) {
		bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer bgCancel()
		_ = c.sessionService.RecordAnswer(bgCtx, c.sessionID, customPrompt, ans)
	}(answerText)
}
