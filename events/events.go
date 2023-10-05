package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
)

type Event string

const (
	CreateUser Event = "CreateUser"
)

type CreateUserEvent struct {
	Username string `json:"username"`
}

const UserStream string = "user_events"

func HandleStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) error {
	stream, err := esdbClient.SubscribeToStream(ctx, UserStream, esdb.SubscribeToStreamOptions{})
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

	for {
		subEvent := stream.Recv()

		if subEvent.EventAppeared != nil {
			if err := handleEvent(ctx, sqlClient, subEvent.EventAppeared); err != nil {
				logger.Error("event handler returned an error", "error", err)
			}
		}

		if subEvent.SubscriptionDropped != nil {
			break
		}
	}

	return nil
}

func handleEvent(ctx context.Context, sqlClient *sql.DB, resolved *esdb.ResolvedEvent) error {
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

func handleCreateUserEvent(ctx context.Context, sqlClient *sql.DB, rawEvent *esdb.RecordedEvent) error {
	var event CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %v", err)
	}

	if _, err := db.InsertUser(ctx, sqlClient, db.User{Username: event.Username}); err != nil {
		return fmt.Errorf("failed to insert user: %v", err)
	}

	return nil
}
