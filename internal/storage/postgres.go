package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDB struct {
	Pool *pgxpool.Pool
}

func NewPostgresDB(dsn string) (*PostgresDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres dsn: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresDB{Pool: pool}, nil
}

func (p *PostgresDB) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}

// AutoMigrate runs the basic initial schema migration if tables don't exist
func (p *PostgresDB) AutoMigrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS users (
		id VARCHAR(64) PRIMARY KEY,
		email VARCHAR(255) UNIQUE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS sessions (
		id VARCHAR(64) PRIMARY KEY,
		user_id VARCHAR(64) NOT NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'active',
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		closed_at TIMESTAMPTZ
	);
	CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

	CREATE TABLE IF NOT EXISTS transcripts (
		id VARCHAR(64) PRIMARY KEY,
		session_id VARCHAR(64) NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		speaker VARCHAR(32) NOT NULL DEFAULT 'user',
		text TEXT NOT NULL,
		is_final BOOLEAN NOT NULL DEFAULT TRUE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_transcripts_session_id ON transcripts(session_id);

	CREATE TABLE IF NOT EXISTS answers (
		id VARCHAR(64) PRIMARY KEY,
		session_id VARCHAR(64) NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		prompt TEXT,
		response TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_answers_session_id ON answers(session_id);
	`
	_, err := p.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("auto migration failed: %w", err)
	}
	return nil
}
