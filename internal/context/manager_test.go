package context_test

import (
	"context"
	"strings"
	"testing"
	"time"

	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenEstimation(t *testing.T) {
	text := "Hello world, this is a test of the token estimator."
	tokens := ctxpkg.EstimateTokens(text)
	assert.Greater(t, tokens, 0)
	assert.Equal(t, len(strings.TrimSpace(text))/4, tokens)
}

func TestContextManager_AddTranscriptAndPrompt(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	summarizer := ctxpkg.NewSummarizer(llmProv)
	mgr := ctxpkg.NewManager(nil, summarizer, 3000, 1000, time.Hour)

	ctx := context.Background()
	sessionID := "test-session-1"

	err := mgr.AddTranscript(ctx, sessionID, "interviewer", "Tell me about your experience with Go.")
	require.NoError(t, err)

	err = mgr.AddTranscript(ctx, sessionID, "candidate", "I have built high throughput microservices with Go and Redis.")
	require.NoError(t, err)

	sCtx, err := mgr.GetContext(ctx, sessionID)
	require.NoError(t, err)
	assert.Equal(t, sessionID, sCtx.SessionID)
	assert.Equal(t, 2, len(sCtx.Messages))
	assert.Greater(t, sCtx.TotalTokens, 0)

	prompt := mgr.BuildPrompt(sCtx, "Provide a follow-up suggestion")
	assert.Contains(t, prompt, "Tell me about your experience with Go")
	assert.Contains(t, prompt, "Provide a follow-up suggestion")
}

func TestContextManager_AutoSummarization(t *testing.T) {
	llmProv := llm.NewFakeLLMProvider(10 * time.Millisecond)
	summarizer := ctxpkg.NewSummarizer(llmProv)
	// Small threshold to force summarization quickly
	mgr := ctxpkg.NewManager(nil, summarizer, 50, 20, time.Hour)

	ctx := context.Background()
	sessionID := "test-session-summarize"

	// Add enough messages to exceed maxContextTokens (50)
	for i := 0; i < 6; i++ {
		err := mgr.AddTranscript(ctx, sessionID, "interviewer", "Can you explain how goroutines and channels communicate in Go?")
		require.NoError(t, err)
		err = mgr.AddTranscript(ctx, sessionID, "candidate", "Goroutines communicate by sharing memory through typed channels safely.")
		require.NoError(t, err)
	}

	// Allow background goroutine to execute
	time.Sleep(100 * time.Millisecond)

	sCtx, err := mgr.GetContext(ctx, sessionID)
	require.NoError(t, err)

	// Summary should now be populated
	assert.NotEmpty(t, sCtx.Summary)
}
