package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	Stream      bool                 `json:"stream,omitempty"`
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

type openAIChatStreamResponse struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
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

func (o *OpenAIProvider) StreamGenerate(ctx context.Context, messages []events.ChatMessage) (<-chan StreamChunk, error) {
	start := time.Now()
	metrics.Default.IncLLMRequests()

	reqBody := openAIChatRequest{
		Model:       o.model,
		Messages:    messages,
		Temperature: 0.7,
		Stream:      true,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		metrics.Default.IncLLMErrors()
		return nil, fmt.Errorf("failed to marshal openai stream request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		metrics.Default.IncLLMErrors()
		return nil, fmt.Errorf("failed to create http stream request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.apiKey)

	resp, err := o.httpClient.Do(req)
	if err != nil {
		metrics.Default.IncLLMErrors()
		return nil, fmt.Errorf("openai stream request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(resp.Body)
		metrics.Default.IncLLMErrors()
		return nil, fmt.Errorf("openai returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	outChan := make(chan StreamChunk, 50)

	go func() {
		defer resp.Body.Close()
		defer close(outChan)
		defer func() {
			metrics.Default.RecordLLMLatency(time.Since(start))
		}()

		reader := bufio.NewReader(resp.Body)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						select {
						case outChan <- StreamChunk{Error: err}:
						case <-ctx.Done():
						}
					}
					return
				}

				line = strings.TrimSpace(line)
				if !strings.HasPrefix(line, "data: ") {
					continue
				}

				data := strings.TrimPrefix(line, "data: ")
				if data == "[DONE]" {
					select {
					case outChan <- StreamChunk{IsFinal: true}:
					case <-ctx.Done():
					}
					return
				}

				var streamResp openAIChatStreamResponse
				if err := json.Unmarshal([]byte(data), &streamResp); err != nil {
					continue
				}

				if len(streamResp.Choices) > 0 {
					delta := streamResp.Choices[0].Delta.Content
					isFinal := streamResp.Choices[0].FinishReason != nil
					if delta != "" || isFinal {
						select {
						case outChan <- StreamChunk{
							Content: delta,
							IsFinal: isFinal,
						}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()

	return outChan, nil
}
