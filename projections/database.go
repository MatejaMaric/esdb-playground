package projections

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
)

type dbProjection struct {
	ctx       context.Context
	sqlClient *sql.DB
}

func NewDatabaseProjection(ctx context.Context, sqlClient *sql.DB) Projection {
	return &dbProjection{
		ctx:       ctx,
		sqlClient: sqlClient,
	}
}

func (p *dbProjection) HandleEvent(resolved *esdb.ResolvedEvent) error {
	if resolved.Event == nil {
		return errors.New("resolved.Event is nil")
	}

	switch resolved.Event.EventType {
	case string(events.CreateUser):
		return p.handleCreateUserEvent(resolved.Event)
	default:
		return fmt.Errorf("unknown event type: %s", resolved.Event.EventType)
	}
}

func (p *dbProjection) handleCreateUserEvent(rawEvent *esdb.RecordedEvent) error {
	var event events.CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user := events.UserStateEvent{
		Username:   event.Username,
		LoginCount: 0,
		Version:    rawEvent.EventNumber,
	}

	if _, err := db.InsertUser(p.ctx, p.sqlClient, user); err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}
