package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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
	if r.client == nil {
		return nil
	}
	return r.client.Close()
}

// AcquireLock attempts to acquire a distributed lock using SET NX EX
func (r *RedisClient) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if r.client == nil {
		return false, fmt.Errorf("redis client is not initialized")
	}
	res, err := r.client.SetNX(ctx, key, "locked", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquire redis lock %s: %w", key, err)
	}
	return res, nil
}

// ReleaseLock releases the distributed lock
func (r *RedisClient) ReleaseLock(ctx context.Context, key string) error {
	if r.client == nil {
		return nil
	}
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("release redis lock %s: %w", key, err)
	}
	return nil
}

// LPush pushes raw bytes into a Redis list
func (r *RedisClient) LPush(ctx context.Context, key string, data []byte) error {
	if r.client == nil {
		return fmt.Errorf("redis client is not initialized")
	}
	if err := r.client.LPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("lpush to redis key %s: %w", key, err)
	}
	return nil
}

// BRPop blocks and pops raw bytes from a Redis list with a timeout
func (r *RedisClient) BRPop(ctx context.Context, timeout time.Duration, key string) ([]byte, error) {
	if r.client == nil {
		return nil, fmt.Errorf("redis client is not initialized")
	}
	res, err := r.client.BRPop(ctx, timeout, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // timeout, queue empty
		}
		return nil, fmt.Errorf("brpop from redis key %s: %w", key, err)
	}

	if len(res) < 2 {
		return nil, nil
	}

	return []byte(res[1]), nil
}
