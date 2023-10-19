package db

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func ConnectToRedis() (*redis.Client, error) {
	opts, err := redis.ParseURL("redis://localhost:6379/")
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res := client.Ping(ctx)
	if res.Err() != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", res.Err())
	}

	return client, nil
}
