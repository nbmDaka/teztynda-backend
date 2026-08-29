package events

import (
	"encoding/json"
	"time"
)

// Inbound message types from WebSocket client
const (
	TypeAudioChunk     = "audio_chunk"
	TypeGenerateAnswer = "generate_answer"
	TypeClientPing     = "ping"
)

// Outbound message types to WebSocket client
const (
	TypeSessionStarted = "session_started"
	TypeTranscript     = "transcript"
	TypeAnswer         = "answer"
	TypeError          = "error"
	TypeServerPong     = "pong"
)

// InboundMessage represents an incoming message from the WebSocket client
type InboundMessage struct {
	Type   string          `json:"type"`
	Data   string          `json:"data,omitempty"`   // base64 encoded audio chunk
	Prompt string          `json:"prompt,omitempty"` // optional custom prompt
	Raw    json.RawMessage `json:"-"`
}

// OutboundMessage represents a standardized outgoing frame to the WebSocket client
type OutboundMessage struct {
	Type      string      `json:"type"`
	SessionID string      `json:"session_id,omitempty"`
	Text      string      `json:"text,omitempty"`
	IsFinal   *bool       `json:"is_final,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp int64       `json:"timestamp,omitempty"`
	Payload   interface{} `json:"payload,omitempty"`
}

// TranscriptEvent is emitted by the STT provider and passed through the audio pipeline
type TranscriptEvent struct {
	SessionID string    `json:"session_id"`
	Text      string    `json:"text"`
	IsFinal   bool      `json:"is_final"`
	CreatedAt time.Time `json:"created_at"`
	Error     error     `json:"-"`
}

// SummarizationTask represents a background summarization job queued in Redis
type SummarizationTask struct {
	SessionID   string    `json:"session_id"`
	TriggeredAt time.Time `json:"triggered_at"`
}

// NewTranscriptMessage creates an OutboundMessage for a transcript event
func NewTranscriptMessage(sessionID, text string, isFinal bool, ts time.Time) OutboundMessage {
	return OutboundMessage{
		Type:      TypeTranscript,
		SessionID: sessionID,
		Text:      text,
		IsFinal:   &isFinal,
		Timestamp: ts.UnixMilli(),
	}
}

// NewAnswerMessage creates an OutboundMessage for an LLM answer
func NewAnswerMessage(sessionID, text string) OutboundMessage {
	return OutboundMessage{
		Type:      TypeAnswer,
		SessionID: sessionID,
		Text:      text,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewErrorMessage creates an OutboundMessage for an error
func NewErrorMessage(errStr string) OutboundMessage {
	return OutboundMessage{
		Type:      TypeError,
		Error:     errStr,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewSessionStartedMessage creates an OutboundMessage when a session connects
func NewSessionStartedMessage(sessionID string) OutboundMessage {
	return OutboundMessage{
		Type:      TypeSessionStarted,
		SessionID: sessionID,
		Timestamp: time.Now().UnixMilli(),
	}
}
