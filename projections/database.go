package projections

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/aggregates"
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

func (p *dbProjection) handleCreateUserEvent(re esdb.RecordedEvent) error {
	user, err := aggregates.User{}.ApplyCreateUser(re)
	if err != nil {
		return err
	}

	if _, err := db.InsertUser(p.ctx, p.sqlClient, user); err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

func (p *dbProjection) handleLoginUserEvent(re esdb.RecordedEvent) error {
	var event events.LoginUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user, err := db.GetUser(p.ctx, p.sqlClient, event.Username)
	if err != nil {
		return err
	}

	user, err = user.ApplyLoginUser(re)
	if err != nil {
		return err
	}

	affectedUsers, err := db.UpdateUser(p.ctx, p.sqlClient, user)
	if err != nil {
		return fmt.Errorf("error updating the user %s: %w", user.Username, err)
	}

	if affectedUsers != 1 {
		return fmt.Errorf("unexpected number of affected users: %d", affectedUsers)
	}

	return nil
}
