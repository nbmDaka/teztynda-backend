package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

var (
	ErrSessionNotFound = errors.New("session not found")
)

type Repository interface {
	SaveSession(ctx context.Context, s *Session) error
	GetSession(ctx context.Context, id string) (*Session, error)
	CloseSession(ctx context.Context, id string) error
	SaveTranscript(ctx context.Context, tr *TranscriptRecord) error
	SaveAnswer(ctx context.Context, ans *AnswerRecord) error
}

type repository struct {
	redisClient *storage.RedisClient
	postgresDB  *storage.PostgresDB
	sessionTTL  time.Duration
}

func NewRepository(redisClient *storage.RedisClient, postgresDB *storage.PostgresDB, sessionTTL time.Duration) Repository {
	return &repository{
		redisClient: redisClient,
		postgresDB:  postgresDB,
		sessionTTL:  sessionTTL,
	}
}

func (r *repository) sessionKey(id string) string {
	return fmt.Sprintf("session:%s", id)
}

func (r *repository) SaveSession(ctx context.Context, s *Session) error {
	// 1. Save in Postgres
	if r.postgresDB != nil && r.postgresDB.Pool != nil {
		query := `
		INSERT INTO sessions (id, user_id, status, created_at, closed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status, closed_at = EXCLUDED.closed_at;
		`
		_, err := r.postgresDB.Pool.Exec(ctx, query, s.ID, s.UserID, string(s.Status), s.CreatedAt, s.ClosedAt)
		if err != nil {
			return fmt.Errorf("failed to persist session in postgres: %w", err)
		}
	}

	// 2. Cache in Redis
	if r.redisClient != nil {
		data, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("failed to marshal session for redis: %w", err)
		}
		err = r.redisClient.Client().Set(ctx, r.sessionKey(s.ID), data, r.sessionTTL).Err()
		if err != nil {
			return fmt.Errorf("failed to cache session in redis: %w", err)
		}
	}

	return nil
}

func (r *repository) GetSession(ctx context.Context, id string) (*Session, error) {
	// 1. Try Redis
	if r.redisClient != nil {
		val, err := r.redisClient.Client().Get(ctx, r.sessionKey(id)).Result()
		if err == nil {
			var s Session
			if err := json.Unmarshal([]byte(val), &s); err == nil {
				return &s, nil
			}
		} else if !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis error when getting session: %w", err)
		}
	}

	// 2. Fallback to Postgres
	if r.postgresDB != nil && r.postgresDB.Pool != nil {
		query := `SELECT id, user_id, status, created_at, closed_at FROM sessions WHERE id = $1`
		row := r.postgresDB.Pool.QueryRow(ctx, query, id)

		var s Session
		var statusStr string
		err := row.Scan(&s.ID, &s.UserID, &statusStr, &s.CreatedAt, &s.ClosedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrSessionNotFound
			}
			return nil, fmt.Errorf("postgres error when getting session: %w", err)
		}
		s.Status = Status(statusStr)
		return &s, nil
	}

	return nil, ErrSessionNotFound
}

func (r *repository) CloseSession(ctx context.Context, id string) error {
	now := time.Now()
	// 1. Update in Postgres
	if r.postgresDB != nil && r.postgresDB.Pool != nil {
		query := `UPDATE sessions SET status = $1, closed_at = $2 WHERE id = $3`
		_, err := r.postgresDB.Pool.Exec(ctx, query, string(StatusClosed), now, id)
		if err != nil {
			return fmt.Errorf("failed to update session in postgres: %w", err)
		}
	}

	// 2. Update in Redis
	s, err := r.GetSession(ctx, id)
	if err == nil && s != nil {
		s.Status = StatusClosed
		s.ClosedAt = &now
		data, _ := json.Marshal(s)
		_ = r.redisClient.Client().Set(ctx, r.sessionKey(id), data, r.sessionTTL).Err()
	}

	return nil
}

func (r *repository) SaveTranscript(ctx context.Context, tr *TranscriptRecord) error {
	if r.postgresDB == nil || r.postgresDB.Pool == nil {
		return nil
	}

	query := `
	INSERT INTO transcripts (id, session_id, speaker, text, is_final, created_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.postgresDB.Pool.Exec(ctx, query, tr.ID, tr.SessionID, tr.Speaker, tr.Text, tr.IsFinal, tr.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to save transcript to postgres: %w", err)
	}
	return nil
}

func (r *repository) SaveAnswer(ctx context.Context, ans *AnswerRecord) error {
	if r.postgresDB == nil || r.postgresDB.Pool == nil {
		return nil
	}

	query := `
	INSERT INTO answers (id, session_id, prompt, response, created_at)
	VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.postgresDB.Pool.Exec(ctx, query, ans.ID, ans.SessionID, ans.Prompt, ans.Response, ans.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to save answer to postgres: %w", err)
	}
	return nil
}
