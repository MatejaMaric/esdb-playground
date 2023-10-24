package handler

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/projections"
)

func HandleUserStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) error {
	opts := esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.StreamFilterType,
			Prefixes: []string{string(events.UserEventsStream)},
		},
	}

	dbProjection := projections.NewDatabaseProjection(ctx, sqlClient)
	streamProjection := projections.NewStreamProjection(ctx, esdbClient)

	handler := func(event esdb.RecordedEvent) error {
		if err := dbProjection.HandleEvent(event); err != nil {
			logger.Error("database projection event handler returned an error", "error", err)
		} else {
			logger.Debug("database projection handled event",
				"EventNumber", event.EventNumber,
				"CommitPosition", event.Position.Commit,
				"PreparePosition", event.Position.Prepare,
			)
		}

		if err := streamProjection.HandleEvent(event); err != nil {
			logger.Error("stream projection event handler returned an error", "error", err)
		} else {
			logger.Debug("stream projection handled event",
				"EventNumber", event.EventNumber,
				"CommitPosition", event.Position.Commit,
				"PreparePosition", event.Position.Prepare,
			)
		}

		return nil
	}

	return db.HandleAllStream(ctx, esdbClient, opts, handler)
}

func AppendCreateUserEvent(ctx context.Context, esdbClient *esdb.Client, event events.CreateUserEvent) (*esdb.WriteResult, error) {
	return db.AppendEvent(
		ctx,
		esdbClient,
		events.UserEventsStream.ForUser(event.Username),
		string(events.CreateUser),
		event,
		esdb.NoStream{},
	)
}

func AppendLoginUserEvent(ctx context.Context, esdbClient *esdb.Client, event events.LoginUserEvent) (*esdb.WriteResult, error) {
	return db.AppendEvent(
		ctx,
		esdbClient,
		events.UserEventsStream.ForUser(event.Username),
		string(events.LoginUser),
		event,
		esdb.StreamExists{},
	)
}
