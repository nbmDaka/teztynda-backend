package stt

import "time"

// TranscriptEvent is emitted by the STT provider and passed through the audio pipeline
type TranscriptEvent struct {
	Sequence  int64     `json:"sequence"`
	SessionID string    `json:"session_id"`
	Text      string    `json:"text"`
	IsFinal   bool      `json:"is_final"`
	CreatedAt time.Time `json:"created_at"`
	Error     error     `json:"-"`
}
