package session

import (
	"time"
)

type Status string

const (
	StatusActive Status = "active"
	StatusClosed Status = "closed"
)

// Session represents a user's real-time connection session
type Session struct {
	ID        string     `json:"session_id" db:"id"`
	UserID    string     `json:"user_id" db:"user_id"`
	Status    Status     `json:"status" db:"status"`
	CreatedAt time.Time  `json:"created_at" db:"created_at"`
	ClosedAt  *time.Time `json:"closed_at,omitempty" db:"closed_at"`
}

// TranscriptRecord represents a persisted final transcript entry in Postgres
type TranscriptRecord struct {
	ID        string    `json:"id" db:"id"`
	SessionID string    `json:"session_id" db:"session_id"`
	Speaker   string    `json:"speaker" db:"speaker"`
	Text      string    `json:"text" db:"text"`
	IsFinal   bool      `json:"is_final" db:"is_final"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// AnswerRecord represents an AI generated response saved in Postgres
type AnswerRecord struct {
	ID        string    `json:"id" db:"id"`
	SessionID string    `json:"session_id" db:"session_id"`
	Prompt    string    `json:"prompt" db:"prompt"`
	Response  string    `json:"response" db:"response"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}
