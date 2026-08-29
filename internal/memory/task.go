package memory

import "time"

const (
	QueueSummarization = "queue:summarization"
)

// SummarizationTask represents a background summarization job queued in Redis
type SummarizationTask struct {
	SessionID      string    `json:"session_id"`
	SummaryVersion int64     `json:"summary_version"`
	TriggeredAt    time.Time `json:"triggered_at"`
}
