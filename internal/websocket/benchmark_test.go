package websocket_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
	wsPkg "github.com/nbmDaka/teztynda-backend/internal/websocket"
	"github.com/stretchr/testify/require"
)

func TestLoad_1000ConcurrentWebSocketClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1000-client load test in short mode")
	}

	// 1. Setup backend infrastructure with zero latency fake providers for high throughput testing
	llmProv := llm.NewFakeLLMProvider(1 * time.Millisecond)
	llmSvc := llm.NewService(llmProv)
	summarizer := memory.NewSummarizer(llmSvc)
	memoryManager := memory.NewManager(nil, summarizer, 3000, 1200, time.Hour)
	sessionRepo := session.NewRepository(nil, nil, time.Hour)
	sessionService := session.NewService(sessionRepo)

	sttFactory := func() stt.STTProvider {
		return stt.NewFakeSTTProvider()
	}

	handler := wsPkg.NewHandler(
		sttFactory,
		llmSvc,
		memoryManager,
		sessionService,
		"bench-secret",
		64*1024,
		10,
	)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Pre-test resource baseline
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	const clientCount = 1000
	var successCount atomic.Int64
	var errorCount atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(clientCount)

	// Concurrency limiter for client dialing to avoid local ephemeral port exhaustion
	sem := make(chan struct{}, 100)

	audioPayload := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})

	for i := 0; i < clientCount; i++ {
		go func(clientID int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			dialer := websocket.Dialer{
				HandshakeTimeout: 5 * time.Second,
			}

			clientWSURL := fmt.Sprintf("%s?user_id=client-%d", wsURL, clientID)
			conn, _, err := dialer.Dial(clientWSURL, nil)
			if err != nil {
				errorCount.Add(1)
				return
			}
			defer conn.Close()

			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

			// Step 1: Read session_started event
			var startMsg events.OutboundMessage
			if err := conn.ReadJSON(&startMsg); err != nil || startMsg.Type != events.TypeSessionStarted {
				errorCount.Add(1)
				return
			}

			// Step 2: Send audio chunks
			for c := 0; c < 4; c++ {
				chunkMsg := events.InboundMessage{
					Type: events.TypeAudioChunk,
					Data: audioPayload,
				}
				if err := conn.WriteJSON(chunkMsg); err != nil {
					errorCount.Add(1)
					return
				}
			}

			// Step 3: Send generate_answer command
			genMsg := events.InboundMessage{
				Type:   events.TypeGenerateAnswer,
				Prompt: "Briefly explain Go concurrency",
			}
			if err := conn.WriteJSON(genMsg); err != nil {
				errorCount.Add(1)
				return
			}

			// Step 4: Read until final answer is received
			gotAnswer := false
			for {
				var outMsg events.OutboundMessage
				if err := conn.ReadJSON(&outMsg); err != nil {
					break
				}
				if outMsg.Type == events.TypeAnswer {
					gotAnswer = true
					break
				}
			}

			if gotAnswer {
				successCount.Add(1)
			} else {
				errorCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Post-test resource metrics
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	goroutinesAfter := runtime.NumGoroutine()

	t.Logf("=== 1,000 Concurrent WebSocket Clients Load Test Results ===")
	t.Logf("Total Clients: %d", clientCount)
	t.Logf("Successful Clients: %d", successCount.Load())
	t.Logf("Failed/Errored Clients: %d", errorCount.Load())
	t.Logf("Total Elapsed Time: %v (Throughput: %.2f clients/sec)", duration, float64(clientCount)/duration.Seconds())
	t.Logf("Goroutines Before: %d | During/After: %d", goroutinesBefore, goroutinesAfter)
	t.Logf("Memory Alloc Before: %.2f MB | Alloc After: %.2f MB (Delta: %.2f MB)",
		float64(memBefore.Alloc)/1024/1024,
		float64(memAfter.Alloc)/1024/1024,
		float64(memAfter.Alloc-memBefore.Alloc)/1024/1024,
	)
	t.Logf("Total Memory Allocated during run: %.2f MB", float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/1024/1024)

	require.Equal(t, int64(clientCount), successCount.Load(), "All 1,000 clients must succeed without drop or deadlock")
	require.Equal(t, int64(0), errorCount.Load(), "Zero errors expected across 1,000 concurrent realtime connections")
}
