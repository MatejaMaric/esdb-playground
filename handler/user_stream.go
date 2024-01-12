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

func HandleUserStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB, readyChan chan<- struct{}) error {
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

	return db.HandleAllStreamsOfType(ctx, logger, esdbClient, events.UserEventsStream, handler, readyChan)
}
