package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/nbmDaka/teztynda-backend/internal/storage"
)

var (
	ErrSessionNotFound = errors.New("session not found")
)

type Repository struct {
	redisClient *storage.RedisClient
	postgresDB  *storage.PostgresDB
	sessionTTL  time.Duration
}

func NewRepository(redisClient *storage.RedisClient, postgresDB *storage.PostgresDB, sessionTTL time.Duration) *Repository {
	return &Repository{
		redisClient: redisClient,
		postgresDB:  postgresDB,
		sessionTTL:  sessionTTL,
	}
}

func (r *Repository) sessionKey(id string) string {
	return fmt.Sprintf("session:%s", id)
}

func (r *Repository) SaveSession(ctx context.Context, s *Session) error {
	// 1. Save in Postgres
	if r.postgresDB != nil {
		query := `
		INSERT INTO sessions (id, user_id, status, created_at, closed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status, closed_at = EXCLUDED.closed_at;
		`
		_, err := r.postgresDB.Exec(ctx, query, s.ID, s.UserID, string(s.Status), s.CreatedAt, s.ClosedAt)
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

func (r *Repository) GetSession(ctx context.Context, id string) (*Session, error) {
	// 1. Try Redis
	if r.redisClient != nil {
		val, err := r.redisClient.Client().Get(ctx, r.sessionKey(id)).Result()
		if err == nil {
			var s Session
			if err := json.Unmarshal([]byte(val), &s); err == nil {
				return &s, nil
			}
		} else if !errors.Is(err, redis.Nil) {
			slog.Warn("Redis error when getting session, falling back to postgres", "error", err, "session_id", id)
		}
	}

	// 2. Fallback to Postgres
	if r.postgresDB != nil {
		query := `SELECT id, user_id, status, created_at, closed_at FROM sessions WHERE id = $1`
		row := r.postgresDB.QueryRow(ctx, query, id)
		if row == nil {
			return nil, ErrSessionNotFound
		}

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

func (r *Repository) CloseSession(ctx context.Context, id string) error {
	now := time.Now().UTC()
	// 1. Update in Postgres
	if r.postgresDB != nil {
		query := `UPDATE sessions SET status = $1, closed_at = $2 WHERE id = $3`
		_, err := r.postgresDB.Exec(ctx, query, string(StatusClosed), now, id)
		if err != nil {
			return fmt.Errorf("failed to update session in postgres: %w", err)
		}
	}

	// 2. Update in Redis
	if r.redisClient != nil {
		s, err := r.GetSession(ctx, id)
		if err == nil && s != nil {
			s.Status = StatusClosed
			s.ClosedAt = &now
			data, err := json.Marshal(s)
			if err == nil {
				if err := r.redisClient.Client().Set(ctx, r.sessionKey(id), data, r.sessionTTL).Err(); err != nil {
					slog.Warn("Failed to update closed session cache in redis", "session_id", id, "error", err)
				}
			}
		}
	}

	return nil
}

func (r *Repository) SaveTranscript(ctx context.Context, tr *TranscriptRecord) error {
	if r.postgresDB == nil {
		return nil
	}

	query := `
	INSERT INTO transcripts (id, session_id, speaker, text, is_final, created_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.postgresDB.Exec(ctx, query, tr.ID, tr.SessionID, tr.Speaker, tr.Text, tr.IsFinal, tr.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to save transcript to postgres: %w", err)
	}
	return nil
}

func (r *Repository) SaveAnswer(ctx context.Context, ans *AnswerRecord) error {
	if r.postgresDB == nil {
		return nil
	}

	query := `
	INSERT INTO answers (id, session_id, prompt, response, created_at)
	VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.postgresDB.Exec(ctx, query, ans.ID, ans.SessionID, ans.Prompt, ans.Response, ans.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to save answer to postgres: %w", err)
	}
	return nil
}

// PruneStaleSessions marks active sessions created earlier than olderThan as closed
func (r *Repository) PruneStaleSessions(ctx context.Context, olderThan time.Duration) (int64, error) {
	if r.postgresDB == nil {
		return 0, nil
	}

	intervalStr := fmt.Sprintf("%d seconds", int(olderThan.Seconds()))
	query := `
	UPDATE sessions
	SET status = 'closed', closed_at = NOW()
	WHERE status = 'active' AND created_at < NOW() - $1::interval;
	`
	tag, err := r.postgresDB.Exec(ctx, query, intervalStr)
	if err != nil {
		return 0, fmt.Errorf("prune stale sessions: %w", err)
	}
	return tag.RowsAffected(), nil
}
