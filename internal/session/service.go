package session

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Store interface {
	SaveSession(ctx context.Context, s *Session) error
	GetSession(ctx context.Context, id string) (*Session, error)
	CloseSession(ctx context.Context, id string) error
	SaveTranscript(ctx context.Context, tr *TranscriptRecord) error
	SaveAnswer(ctx context.Context, ans *AnswerRecord) error
	PruneStaleSessions(ctx context.Context, olderThan time.Duration) (int64, error)
}

type Service struct {
	repo Store
}

func NewService(repo Store) *Service {
	return &Service{
		repo: repo,
	}
}

func (s *Service) CreateSession(ctx context.Context, userID string) (*Session, error) {
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
		return nil, fmt.Errorf("create session: %w", err)
	}

	return sess, nil
}

func (s *Service) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	return s.repo.GetSession(ctx, sessionID)
}

func (s *Service) CloseSession(ctx context.Context, sessionID string) error {
	return s.repo.CloseSession(ctx, sessionID)
}

func (s *Service) RecordTranscript(ctx context.Context, sessionID, speaker, text string, isFinal bool) error {
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

func (s *Service) RecordAnswer(ctx context.Context, sessionID, prompt, response string) error {
	ans := &AnswerRecord{
		ID:        uuid.New().String(),
		SessionID: sessionID,
		Prompt:    prompt,
		Response:  response,
		CreatedAt: time.Now().UTC(),
	}
	return s.repo.SaveAnswer(ctx, ans)
}

func (s *Service) PruneStaleSessions(ctx context.Context, olderThan time.Duration) (int64, error) {
	return s.repo.PruneStaleSessions(ctx, olderThan)
}
