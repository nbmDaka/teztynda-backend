package session

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Service interface {
	CreateSession(ctx context.Context, userID string) (*Session, error)
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	CloseSession(ctx context.Context, sessionID string) error
	RecordTranscript(ctx context.Context, sessionID, speaker, text string, isFinal bool) error
	RecordAnswer(ctx context.Context, sessionID, prompt, response string) error
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &service{
		repo: repo,
	}
}

func (s *service) CreateSession(ctx context.Context, userID string) (*Session, error) {
	if userID == "" {
		userID = "anonymous-" + uuid.New().String()[:8]
	}

	sess := &Session{
		ID:        uuid.New().String(),
		UserID:    userID,
		Status:    StatusActive,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.repo.SaveSession(ctx, sess); err != nil {
		return nil, err
	}

	return sess, nil
}

func (s *service) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	return s.repo.GetSession(ctx, sessionID)
}

func (s *service) CloseSession(ctx context.Context, sessionID string) error {
	return s.repo.CloseSession(ctx, sessionID)
}

func (s *service) RecordTranscript(ctx context.Context, sessionID, speaker, text string, isFinal bool) error {
	tr := &TranscriptRecord{
		ID:        uuid.New().String(),
		SessionID: sessionID,
		Speaker:   speaker,
		Text:      text,
		IsFinal:   isFinal,
		CreatedAt: time.Now().UTC(),
	}
	return s.repo.SaveTranscript(ctx, tr)
}

func (s *service) RecordAnswer(ctx context.Context, sessionID, prompt, response string) error {
	ans := &AnswerRecord{
		ID:        uuid.New().String(),
		SessionID: sessionID,
		Prompt:    prompt,
		Response:  response,
		CreatedAt: time.Now().UTC(),
	}
	return s.repo.SaveAnswer(ctx, ans)
}
