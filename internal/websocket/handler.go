package websocket

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
	"github.com/nbmDaka/teztynda-backend/internal/memory"
	"github.com/nbmDaka/teztynda-backend/internal/session"
	"github.com/nbmDaka/teztynda-backend/internal/stt"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 32,
	WriteBufferSize: 1024 * 32,
	CheckOrigin: func(r *http.Request) bool {
		// In production, validate against allowed origins
		return true
	},
}

type Handler struct {
	sttFactory            stt.ProviderFactory
	llmService            *llm.Service
	memoryManager         *memory.Manager
	sessionService        *session.Service
	jwtSecret             string
	maxAudioChunkBytes    int
	maxConcurrentLLMCalls int
}

func NewHandler(
	sttFactory stt.ProviderFactory,
	llmService *llm.Service,
	memoryManager *memory.Manager,
	sessionService *session.Service,
	jwtSecret string,
	maxAudioChunkBytes int,
	maxConcurrentLLMCalls int,
) *Handler {
	return &Handler{
		sttFactory:            sttFactory,
		llmService:            llmService,
		memoryManager:         memoryManager,
		sessionService:        sessionService,
		jwtSecret:             jwtSecret,
		maxAudioChunkBytes:    maxAudioChunkBytes,
		maxConcurrentLLMCalls: maxConcurrentLLMCalls,
	}
}

// authenticate extracts user identity from query parameters or Authorization header
func (h *Handler) authenticate(r *http.Request) (string, error) {
	// 1. Check query param user_id or token
	userID := r.URL.Query().Get("user_id")
	token := r.URL.Query().Get("token")

	if token == "" {
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	// In a real JWT setup, parse and validate token claims here.
	// For dev/flexibility, if user_id or token is supplied, use it; otherwise fallback to anonymous ID
	if userID != "" {
		return userID, nil
	}
	if token != "" {
		return "user-" + token[:min(len(token), 8)], nil
	}

	return "", nil
}

// ServeHTTP handles GET /ws/realtime
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID, err := h.authenticate(r)
	if err != nil {
		slog.Warn("Authentication failed for WebSocket handshake", "error", err)
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade websocket connection", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 1. Session Manager: create session and track user metadata
	sess, err := h.sessionService.CreateSession(ctx, userID)
	if err != nil {
		slog.Error("Failed to initialize session", "error", err)
		_ = ws.WriteJSON(map[string]string{"error": "failed to initialize session"})
		_ = ws.Close()
		return
	}

	slog.Info("New WebSocket connection accepted", "session_id", sess.ID, "user_id", sess.UserID)

	// 2. Audio Pipeline: initialize STT provider directly through STT factory
	sttProvider := h.sttFactory()
	if err := sttProvider.StartSession(r.Context(), sess.ID); err != nil {
		slog.Error("Failed to start STT streaming session", "session_id", sess.ID, "error", err)
		_ = ws.WriteJSON(map[string]string{"error": "failed to start stt streaming session"})
		_ = ws.Close()
		return
	}

	// 3. Instantiate and run Connection pumps (readPump, writePump, transcriptPump, audioPump)
	conn := NewConnection(
		sess.ID,
		sess.UserID,
		ws,
		sttProvider,
		h.llmService,
		h.memoryManager,
		h.sessionService,
		h.maxAudioChunkBytes,
		h.maxConcurrentLLMCalls,
	)

	conn.Start()
}
