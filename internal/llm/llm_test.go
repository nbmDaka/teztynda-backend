package llm_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeLLMProvider_GenerateAndStream(t *testing.T) {
	prov := llm.NewFakeLLMProvider(10 * time.Millisecond)
	svc := llm.NewService(prov)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages := []llm.ChatMessage{
		{Role: "user", Content: "How do you approach scaling WebSockets to 10000 connections?"},
	}

	// 1. Test Generate (batch)
	ans, err := svc.GenerateAnswer(ctx, messages)
	require.NoError(t, err)
	assert.NotEmpty(t, ans)
	assert.Contains(t, ans, "10,000")

	// 2. Test StreamAnswer (streaming)
	stream, err := svc.StreamAnswer(ctx, messages)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var sb strings.Builder
	for chunk := range stream {
		require.NoError(t, chunk.Error)
		sb.WriteString(chunk.Content)
	}

	streamedAnswer := sb.String()
	assert.NotEmpty(t, streamedAnswer)
	assert.Contains(t, streamedAnswer, "10,000")
}

func TestFakeLLMProvider_GenerateSummary(t *testing.T) {
	prov := llm.NewFakeLLMProvider(10 * time.Millisecond)
	svc := llm.NewService(prov)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	summary, err := svc.GenerateSummary(ctx, "Previous context", "Candidate discussed Go channels and mutexes.")
	require.NoError(t, err)
	assert.NotEmpty(t, summary)
	assert.Contains(t, strings.ToLower(summary), "summary")
}
