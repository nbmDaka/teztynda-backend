package stt

import (
	"context"
	"fmt"
	"log/slog"
)

type Service interface {
	CreateProvider(ctx context.Context, sessionID string) (STTProvider, error)
	ProcessAudio(provider STTProvider, chunk []byte) error
	CloseProvider(provider STTProvider) error
}

type service struct {
	factory ProviderFactory
}

func NewService(factory ProviderFactory) Service {
	return &service{
		factory: factory,
	}
}

func (s *service) CreateProvider(ctx context.Context, sessionID string) (STTProvider, error) {
	provider := s.factory()
	if err := provider.StartSession(ctx, sessionID); err != nil {
		return nil, fmt.Errorf("failed to start STT provider session: %w", err)
	}
	slog.Info("STT streaming session initialized", "session_id", sessionID)
	return provider, nil
}

func (s *service) ProcessAudio(provider STTProvider, chunk []byte) error {
	if provider == nil {
		return fmt.Errorf("stt provider is nil")
	}
	return provider.SendAudio(chunk)
}

func (s *service) CloseProvider(provider STTProvider) error {
	if provider == nil {
		return nil
	}
	return provider.Close()
}
