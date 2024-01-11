package handler

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
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

	retryCounter := 0
	lastRetry := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			opts := esdb.SubscribeToAllOptions{
				From: lastProcessedEvent,
				Filter: &esdb.SubscriptionFilter{
					Type:     esdb.StreamFilterType,
					Prefixes: []string{string(events.UserEventsStream)},
				},
			}

			if err := db.HandleAllStream(ctx, esdbClient, opts, handler); err != nil {
				logger.Error("handling all stream returned an error", "error", err)

				if time.Since(lastRetry) >= 5*time.Minute {
					logger.Debug("more than 5 minutes passed since last retry, resetting counter",
						"lastRetry", lastRetry,
						"retryCounter", retryCounter,
					)
					retryCounter = 0
				}
				if retryCounter >= 5 {
					return errors.New("retired 5 times in the last 5 minutes, failing...")
				}
				time.Sleep(time.Second)
				lastRetry = time.Now()
				retryCounter++
			}
		}
	}
}
