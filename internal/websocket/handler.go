package websocket

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	ctxpkg "github.com/nbmDaka/teztynda-backend/internal/context"
	"github.com/nbmDaka/teztynda-backend/internal/llm"
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
	sttFactory     stt.ProviderFactory
	llmProvider    llm.LLMProvider
	contextManager ctxpkg.Manager
	sessionService session.Service
}

func NewHandler(
	sttFactory stt.ProviderFactory,
	llmProvider llm.LLMProvider,
	contextManager ctxpkg.Manager,
	sessionService session.Service,
) *Handler {
	return &Handler{
		sttFactory:     sttFactory,
		llmProvider:    llmProvider,
		contextManager: contextManager,
		sessionService: sessionService,
	}
}

// ServeHTTP handles GET /ws/realtime
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade websocket", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 1. Create Session
	sess, err := h.sessionService.CreateSession(ctx, userID)
	if err != nil {
		slog.Error("Failed to create session", "error", err)
		_ = ws.WriteJSON(map[string]string{"error": "failed to initialize session"})
		_ = ws.Close()
		return
	}

	slog.Info("New WebSocket connection established", "session_id", sess.ID, "user_id", sess.UserID)

	// 2. Initialize STT Provider for this stream
	sttProvider := h.sttFactory()
	if err := sttProvider.StartSession(context.Background(), sess.ID); err != nil {
		slog.Error("Failed to start STT provider session", "session_id", sess.ID, "error", err)
		_ = ws.WriteJSON(map[string]string{"error": "failed to start stt session"})
		_ = ws.Close()
		return
	}

	// 3. Instantiate and run Connection pumps
	conn := NewConnection(
		sess.ID,
		sess.UserID,
		ws,
		sttProvider,
		h.llmProvider,
		h.contextManager,
		h.sessionService,
	)

	conn.Start()
}
