package llm

// ChatMessage represents a structured role-based message for LLM interactions
type ChatMessage struct {
	Role    string `json:"role"` // "system", "user", "assistant"
	Content string `json:"content"`
}
