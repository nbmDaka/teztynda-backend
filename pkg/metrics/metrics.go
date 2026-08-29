package metrics

import (
	"sync/atomic"
	"time"
)

// Metrics holds operational metrics for observability and telemetry
type Metrics struct {
	ActiveWebSocketConnections int64
	ActiveSessions             int64
	TranscriptEventsTotal      int64
	AudioChunksProcessedTotal  int64
	AudioQueueSize             int64
	AudioChunksDroppedTotal    int64
	LLMRequestsTotal           int64
	LLMErrorsTotal             int64
	STTErrorsTotal             int64
	ErrorsTotal                int64
	LastSTTLatencyMs           int64
	LastLLMLatencyMs           int64
}

var Default = &Metrics{}

func (m *Metrics) IncActiveConnections() {
	atomic.AddInt64(&m.ActiveWebSocketConnections, 1)
}

func (m *Metrics) DecActiveConnections() {
	atomic.AddInt64(&m.ActiveWebSocketConnections, -1)
}

func (m *Metrics) IncActiveSessions() {
	atomic.AddInt64(&m.ActiveSessions, 1)
}

func (m *Metrics) DecActiveSessions() {
	atomic.AddInt64(&m.ActiveSessions, -1)
}

func (m *Metrics) IncTranscriptEvents() {
	atomic.AddInt64(&m.TranscriptEventsTotal, 1)
}

func (m *Metrics) IncAudioChunks() {
	atomic.AddInt64(&m.AudioChunksProcessedTotal, 1)
}

func (m *Metrics) IncAudioDropped() {
	atomic.AddInt64(&m.AudioChunksDroppedTotal, 1)
	atomic.AddInt64(&m.ErrorsTotal, 1)
}

func (m *Metrics) SetAudioQueueSize(size int64) {
	atomic.StoreInt64(&m.AudioQueueSize, size)
}

func (m *Metrics) IncLLMRequests() {
	atomic.AddInt64(&m.LLMRequestsTotal, 1)
}

func (m *Metrics) IncLLMErrors() {
	atomic.AddInt64(&m.LLMErrorsTotal, 1)
	atomic.AddInt64(&m.ErrorsTotal, 1)
}

func (m *Metrics) IncSTTErrors() {
	atomic.AddInt64(&m.STTErrorsTotal, 1)
	atomic.AddInt64(&m.ErrorsTotal, 1)
}

func (m *Metrics) IncErrors() {
	atomic.AddInt64(&m.ErrorsTotal, 1)
}

func (m *Metrics) RecordSTTLatency(d time.Duration) {
	atomic.StoreInt64(&m.LastSTTLatencyMs, d.Milliseconds())
}

func (m *Metrics) RecordLLMLatency(d time.Duration) {
	atomic.StoreInt64(&m.LastLLMLatencyMs, d.Milliseconds())
}

func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"active_websocket_connections": atomic.LoadInt64(&m.ActiveWebSocketConnections),
		"active_sessions":              atomic.LoadInt64(&m.ActiveSessions),
		"audio_chunks_processed":       atomic.LoadInt64(&m.AudioChunksProcessedTotal),
		"audio_queue_size":             atomic.LoadInt64(&m.AudioQueueSize),
		"audio_chunks_dropped":         atomic.LoadInt64(&m.AudioChunksDroppedTotal),
		"transcript_events_total":      atomic.LoadInt64(&m.TranscriptEventsTotal),
		"llm_requests_total":           atomic.LoadInt64(&m.LLMRequestsTotal),
		"stt_latency_ms":               atomic.LoadInt64(&m.LastSTTLatencyMs),
		"llm_latency_ms":               atomic.LoadInt64(&m.LastLLMLatencyMs),
		"llm_errors_total":             atomic.LoadInt64(&m.LLMErrorsTotal),
		"stt_errors_total":             atomic.LoadInt64(&m.STTErrorsTotal),
		"errors_total":                 atomic.LoadInt64(&m.ErrorsTotal),
	}
}
