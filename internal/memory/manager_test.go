package memory_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenEstimation(t *testing.T) {
	text := "Hello world, this is a test of the token estimator."
	tokens := memory.EstimateTokens(text)
	assert.Greater(t, tokens, 0)
	assert.Equal(t, len(strings.TrimSpace(text))/4, tokens)
}

func TestContextManager_3LevelMemoryAndPrompt(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	llmSvc := llm.NewService(llmProv)
	summarizer := memory.NewSummarizer(llmSvc)
	mgr := memory.NewManager(nil, summarizer, 3000, 1200, time.Hour)

	ctx := context.Background()
	sessionID := "test-session-1"

	// 1. Current turn update (partial)
	err := mgr.UpdateCurrentTurn(ctx, sessionID, "interviewer", "Tell me about your")
	require.NoError(t, err)

	sCtx, err := mgr.GetContext(ctx, sessionID)
	require.NoError(t, err)
	assert.NotNil(t, sCtx.CurrentTurn)
	assert.Equal(t, "Tell me about your", sCtx.CurrentTurn.Text)

	// 2. Final transcript commit
	err = mgr.AddTranscript(ctx, sessionID, "interviewer", "Tell me about your experience with Go.")
	require.NoError(t, err)

	err = mgr.AddTranscript(ctx, sessionID, "candidate", "I have built high throughput microservices with Go and Redis.")
	require.NoError(t, err)

	sCtx, err = mgr.GetContext(ctx, sessionID)
	require.NoError(t, err)
	assert.Nil(t, sCtx.CurrentTurn) // cleared
	assert.Equal(t, 2, len(sCtx.ShortMemory))
	assert.Greater(t, sCtx.TotalTokens, 0)

	prompt := mgr.BuildPrompt(sCtx, "Generate best answer")
	assert.Contains(t, prompt, "Tell me about your experience with Go")
	assert.Contains(t, prompt, "Generate best answer")
}

func TestContextManager_AutoSummarization(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	llmSvc := llm.NewService(llmProv)
	summarizer := memory.NewSummarizer(llmSvc)
	// Small threshold to trigger auto-summarization
	mgr := memory.NewManager(nil, summarizer, 50, 20, time.Hour)

	ctx := context.Background()
	sessionID := "test-session-summarize"

	for i := 0; i < 6; i++ {
		err := mgr.AddTranscript(ctx, sessionID, "interviewer", "Can you explain how goroutines and channels communicate in Go?")
		require.NoError(t, err)
		err = mgr.AddTranscript(ctx, sessionID, "candidate", "Goroutines communicate by sharing memory through typed channels safely.")
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	sCtx, err := mgr.GetContext(ctx, sessionID)
	require.NoError(t, err)

	// Long memory summary should now be populated
	assert.NotEmpty(t, sCtx.LongMemory)
}
