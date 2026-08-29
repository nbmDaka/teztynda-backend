package websocket_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
	wsPkg "github.com/nbmDaka/teztynda-backend/internal/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketHandler_EndToEndFlow(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	summarizer := ctxpkg.NewSummarizer(llmProv)
	contextManager := ctxpkg.NewManager(nil, summarizer, 3000, 1000, time.Hour)
	sessionRepo := session.NewRepository(nil, nil, time.Hour)
	sessionService := session.NewService(sessionRepo)

	sttFactory := func() stt.STTProvider {
		return stt.NewFakeSTTProvider()
	}

	handler := wsPkg.NewHandler(sttFactory, llmProv, contextManager, sessionService)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?user_id=test-user"

	dialer := websocket.Dialer{}
	conn, resp, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	// 1. First message should be session_started
	var msg1 events.OutboundMessage
	err = conn.ReadJSON(&msg1)
	require.NoError(t, err)
	assert.Equal(t, events.TypeSessionStarted, msg1.Type)
	assert.NotEmpty(t, msg1.SessionID)

	// 2. Send audio chunks
	audioPayload := base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0x03})
	for i := 0; i < 4; i++ {
		audioMsg := events.InboundMessage{
			Type: events.TypeAudioChunk,
			Data: audioPayload,
		}
		err = conn.WriteJSON(audioMsg)
		require.NoError(t, err)
	}

	// 3. Read transcript message
	var msg2 events.OutboundMessage
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	err = conn.ReadJSON(&msg2)
	require.NoError(t, err)
	assert.Equal(t, events.TypeTranscript, msg2.Type)
	assert.NotEmpty(t, msg2.Text)

	// 4. Send generate_answer request
	genMsg := events.InboundMessage{
		Type: events.TypeGenerateAnswer,
	}
	err = conn.WriteJSON(genMsg)
	require.NoError(t, err)

	// 5. Expect answer response
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
