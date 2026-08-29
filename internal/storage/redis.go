package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/nbmDaka/teztynda-backend/internal/events"
)

const (
	QueueSummarization = "queue:summarization"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr, password string, db int) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", addr, err)
	}

	return &RedisClient{client: rdb}, nil
}

func (r *RedisClient) Client() *redis.Client {
	return r.client
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}

// AcquireLock attempts to acquire a distributed lock using SET NX EX
func (r *RedisClient) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, "locked", ttl).Result()
}

// ReleaseLock releases the distributed lock
func (r *RedisClient) ReleaseLock(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// PushSummarizationTask enqueues a background summarization job into Redis list
func (r *RedisClient) PushSummarizationTask(ctx context.Context, task events.SummarizationTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return r.client.LPush(ctx, QueueSummarization, data).Err()
}

// PopSummarizationTask blocks and pops a summarization job from the Redis queue with a timeout
func (r *RedisClient) PopSummarizationTask(ctx context.Context, timeout time.Duration) (*events.SummarizationTask, error) {
	res, err := r.client.BRPop(ctx, timeout, QueueSummarization).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // timeout, queue empty
		}
		return nil, err
	}

	if len(res) < 2 {
		return nil, nil
	}

	var task events.SummarizationTask
	if err := json.Unmarshal([]byte(res[1]), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal summarization task: %w", err)
	}

	return &task, nil
}
