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
	LLMRequestsTotal           int64
	LLMErrorsTotal             int64
	STTErrorsTotal             int64
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

func (m *Metrics) IncLLMRequests() {
	atomic.AddInt64(&m.LLMRequestsTotal, 1)
}

func (m *Metrics) IncLLMErrors() {
	atomic.AddInt64(&m.LLMErrorsTotal, 1)
}

func (m *Metrics) IncSTTErrors() {
	atomic.AddInt64(&m.STTErrorsTotal, 1)
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
		"transcript_events_total":      atomic.LoadInt64(&m.TranscriptEventsTotal),
		"audio_chunks_processed_total": atomic.LoadInt64(&m.AudioChunksProcessedTotal),
		"llm_requests_total":           atomic.LoadInt64(&m.LLMRequestsTotal),
		"llm_errors_total":             atomic.LoadInt64(&m.LLMErrorsTotal),
		"stt_errors_total":             atomic.LoadInt64(&m.STTErrorsTotal),
		"last_stt_latency_ms":          atomic.LoadInt64(&m.LastSTTLatencyMs),
		"last_llm_latency_ms":          atomic.LoadInt64(&m.LastLLMLatencyMs),
	}
}
