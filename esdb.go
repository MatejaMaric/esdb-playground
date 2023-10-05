package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

type Event string

const (
	CreateUser Event = "CreateUser"
)

type CreateUserEvent struct {
	Username string `json:"username"`
}

const StreamID string = "user_events"

func handleEvents(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) error {
	stream, err := esdbClient.SubscribeToStream(ctx, StreamID, esdb.SubscribeToStreamOptions{})
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %v", err)
	}
	defer stream.Close()

	handleCreateUser := func(data []byte) {
		var event CreateUserEvent
		if err := json.Unmarshal(data, &event); err != nil {
			logger.Error("failed to unmarshal event", "error", err)
			return
		}

		if _, err := insertUser(ctx, sqlClient, User{Username: event.Username}); err != nil {
			logger.Error("failed to insert user", "error", err)
			return
		}
	}

	handleEvent := func(resolved *esdb.ResolvedEvent) {
		if resolved.Event == nil {
			logger.Error("resolved.Event is nil")
			return
		}

		switch resolved.Event.EventType {
		case string(CreateUser):
			handleCreateUser(resolved.Event.Data)
		}
	}

	for {
		subEvent := stream.Recv()
		if subEvent.EventAppeared != nil {
			handleEvent(subEvent.EventAppeared)
		}
		if subEvent.SubscriptionDropped != nil {
			break
		}
	}

	return nil
}
