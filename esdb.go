package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	defer func() {
		if err := stream.Close(); err != nil {
			logger.Error("closing the stream resulted in an error", "error", err)
		} else {
			logger.Debug("stream successfully closed")
		}
	}()

	handleEvent := func(resolved *esdb.ResolvedEvent) error {
		if resolved.Event == nil {
			return errors.New("resolved.Event is nil")
		}

		switch resolved.Event.EventType {
		case string(CreateUser):
			return handleCreateUserEvent(ctx, sqlClient, resolved.Event)
		default:
			return fmt.Errorf("unknown event type: %s", resolved.Event.EventType)
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

func handleCreateUserEvent(ctx context.Context, sqlClient *sql.DB, rawEvent *esdb.RecordedEvent) error {
	var event CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %v", err)
	}

	if _, err := insertUser(ctx, sqlClient, User{Username: event.Username}); err != nil {
		return fmt.Errorf("failed to insert user: %v", err)
	}

	return nil
}
