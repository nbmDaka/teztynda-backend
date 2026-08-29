package websocket_test

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
	wsPkg "github.com/nbmDaka/teztynda-backend/internal/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketHandler_EndToEndFlow(t *testing.T) {
	// 1. Setup Providers & Services
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
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
		"test-jwt-secret",
		64*1024,
		2,
	)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?user_id=test-user"

	dialer := websocket.Dialer{}
	conn, resp, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	// 2. Receive session_started event
	var msg1 events.OutboundMessage
	err = conn.ReadJSON(&msg1)
	require.NoError(t, err)
	assert.Equal(t, events.TypeSessionStarted, msg1.Type)
	assert.NotEmpty(t, msg1.SessionID)

	// 3. Stream audio chunks
	audioPayload := base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0x03})
	for i := 0; i < 4; i++ {
		audioMsg := events.InboundMessage{
			Type: events.TypeAudioChunk,
			Data: audioPayload,
		}
		err = conn.WriteJSON(audioMsg)
		require.NoError(t, err)
	}

	// 4. Receive transcript event
	var msg2 events.OutboundMessage
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	err = conn.ReadJSON(&msg2)
	require.NoError(t, err)
	assert.Equal(t, events.TypeTranscript, msg2.Type)
	assert.NotEmpty(t, msg2.Text)

	// 5. Send generate_answer command
	genMsg := events.InboundMessage{
		Type: events.TypeGenerateAnswer,
	}
	err = conn.WriteJSON(genMsg)
	require.NoError(t, err)

	// 6. Receive answer event
	var msg3 events.OutboundMessage
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var m events.OutboundMessage
		err = conn.ReadJSON(&m)
		require.NoError(t, err)
		if m.Type == events.TypeAnswer {
			msg3 = m
			break
		}
	}
	assert.Equal(t, events.TypeAnswer, msg3.Type)
	assert.NotEmpty(t, msg3.Text)
}
