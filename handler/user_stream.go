package handler

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/cenkalti/backoff/v4"

	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/projections"
)

func HandleUserStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) error {
	var lastProcessedEvent esdb.Position = esdb.StartPosition

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

		lastProcessedEvent = event.Position

		return nil
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = 3 * time.Second

	var op backoff.Operation = func() error {
		opts := esdb.SubscribeToAllOptions{
			From: lastProcessedEvent,
			Filter: &esdb.SubscriptionFilter{
				Type:     esdb.StreamFilterType,
				Prefixes: []string{string(events.UserEventsStream)},
			},
		}

		return db.HandleAllStream(ctx, esdbClient, opts, handler)
	}

	var notify backoff.Notify = func(err error, d time.Duration) {
		logger.Warn("HandleAllStream return an error, retrying",
			"error", err,
			"duration", d,
		)
	}

	/*
		`backoff.RetryNotify` most probably doesn't work with long
		living (blocking) functions. If I remember correctly, if the
		error happens after successful start (for example, after 12s of
		successful event processing), it's not event going to return en
		error, only hang.
	*/
	return backoff.RetryNotify(op, backoff.WithContext(eb, ctx), notify)
}
