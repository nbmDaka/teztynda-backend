package websocket_test

import (
	"testing"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
	wsPkg "github.com/nbmDaka/teztynda-backend/internal/websocket"
	"github.com/stretchr/testify/assert"
)

func TestConnection_InitializationAndSemaphore(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	llmSvc := llm.NewService(llmProv)
	summarizer := memory.NewSummarizer(llmSvc)
	memoryManager := memory.NewManager(nil, summarizer, 3000, 1200, time.Hour)
	sessionRepo := session.NewRepository(nil, nil, time.Hour)
	sessionService := session.NewService(sessionRepo)

	sttProv := stt.NewFakeSTTProvider()

	conn := wsPkg.NewConnection(
		"sess-test-1",
		"user-1",
		nil,
		sttProv,
		llmSvc,
		memoryManager,
		sessionService,
		64*1024,
		1,
	)

	assert.NotNil(t, conn)
}
