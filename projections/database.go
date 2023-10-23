package projections

import (
	"context"
	"database/sql"
	"encoding/json"
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

func (p *dbProjection) HandleEvent(event esdb.RecordedEvent) error {
	switch event.EventType {
	case string(events.CreateUser):
		return p.handleCreateUserEvent(event)
	case string(events.LoginUser):
		return p.handleLoginUserEvent(event)
	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}

func (p *dbProjection) handleCreateUserEvent(rawEvent esdb.RecordedEvent) error {
	var event events.CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user := events.UserStateEvent{
		Username:   event.Username,
		Email:      event.Email,
		LoginCount: 0,
		Version:    rawEvent.EventNumber,
	}

	if _, err := db.InsertUser(p.ctx, p.sqlClient, user); err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

func (p *dbProjection) handleLoginUserEvent(rawEvent esdb.RecordedEvent) error {
	var event events.LoginUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user, err := db.GetUser(p.ctx, p.sqlClient, event.Username)
	if err != nil {
		return fmt.Errorf("failed to get user %s: %w", event.Username, err)
	}

	if user.Version != (rawEvent.EventNumber - 1) {
		return fmt.Errorf("expected version %d, got %d ", rawEvent.EventNumber-1, user.Version)
	}

	user.LoginCount++
	user.Version = rawEvent.EventNumber

	affectedUsers, err := db.UpdateUser(p.ctx, p.sqlClient, user)
	if err != nil {
		return fmt.Errorf("error updating the user %s: %w", user.Username, err)
	}

	if affectedUsers != 1 {
		return fmt.Errorf("unexpected number of affected users: %d", affectedUsers)
	}

	return nil
}
