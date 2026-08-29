package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nbmDaka/teztynda-backend/internal/events"
	"github.com/nbmDaka/teztynda-backend/pkg/metrics"
)

type OpenAIProvider struct {
	apiKey     string
	model      string
	httpClient *http.Client
}

type openAIChatRequest struct {
	Model       string               `json:"model"`
	Messages    []events.ChatMessage `json:"messages"`
	Temperature float32              `json:"temperature"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func NewOpenAIProvider(apiKey, model string) *OpenAIProvider {
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &OpenAIProvider{
		apiKey: apiKey,
		model:  model,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (o *OpenAIProvider) Generate(ctx context.Context, messages []events.ChatMessage) (string, error) {
	start := time.Now()
	metrics.Default.IncLLMRequests()

	reqBody := openAIChatRequest{
		Model:       o.model,
		Messages:    messages,
		Temperature: 0.7,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("failed to marshal openai request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.apiKey)

	resp, err := o.httpClient.Do(req)
	if err != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("openai request failed: %w", err)
	}
	defer resp.Body.Close()

	metrics.Default.RecordLLMLatency(time.Since(start))

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("failed to read openai response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("openai returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp openAIChatResponse
	if err := json.Unmarshal(bodyBytes, &chatResp); err != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("failed to parse openai response json: %w", err)
	}

	if chatResp.Error != nil {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("openai api error: %s", chatResp.Error.Message)
	}

	if len(chatResp.Choices) == 0 {
		metrics.Default.IncLLMErrors()
		return "", fmt.Errorf("openai returned no choices")
	}

	return chatResp.Choices[0].Message.Content, nil
}
