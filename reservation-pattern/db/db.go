package db

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/redis/go-redis/v9"
)

func ConnectToEventStoreDB() (*esdb.Client, error) {
	const connectionStr string = "esdb://localhost:2113?tls=false"

	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EventStoreDB connection string: %w", err)
	}

	return esdb.NewClient(esdbConf)
}

func ConnectToRedis() (*redis.Client, error) {
	opts, err := redis.ParseURL("redis://localhost:6379/")
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	return client, nil
}
