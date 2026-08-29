package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv     string
	Port       string
	Host       string
	LogLevel   string

	// Redis
	RedisURL      string
	RedisPassword string
	RedisDB       int

	// PostgreSQL
	PostgresDSN string

	// STT & LLM
	STTProvider     string // "fake", "deepgram"
	DeepgramAPIKey  string
	LLMProvider     string // "fake", "openai"
	OpenAIAPIKey    string
	OpenAIModel     string

	// Context Memory Limits
	MaxContextTokens   int           // Threshold to trigger auto-summarization (e.g., 3000)
	ShortMemoryTokens  int           // Desired token window for recent raw messages (e.g., 1000)
	SessionTTL         time.Duration // Redis session expiration (e.g., 24h)
}

// Load loads configuration from environment variables (and .env file if present)
func Load() (*Config, error) {
	// Attempt to load .env file; ignore if missing
	_ = godotenv.Load()

	cfg := &Config{
		AppEnv:            getEnv("APP_ENV", "development"),
		Port:              getEnv("PORT", "8080"),
		Host:              getEnv("HOST", "0.0.0.0"),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
		RedisURL:          getEnv("REDIS_URL", "localhost:6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RedisDB:           getEnvAsInt("REDIS_DB", 0),
		PostgresDSN:       getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/teztynda?sslmode=disable"),
		STTProvider:       getEnv("STT_PROVIDER", "fake"),
		DeepgramAPIKey:    getEnv("DEEPGRAM_API_KEY", ""),
		LLMProvider:       getEnv("LLM_PROVIDER", "fake"),
		OpenAIAPIKey:      getEnv("OPENAI_API_KEY", ""),
		OpenAIModel:       getEnv("OPENAI_MODEL", "gpt-4o-mini"),
		MaxContextTokens:  getEnvAsInt("MAX_CONTEXT_TOKENS", 3000),
		ShortMemoryTokens: getEnvAsInt("SHORT_MEMORY_TOKENS", 1000),
		SessionTTL:        time.Hour * 24,
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists && val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}
