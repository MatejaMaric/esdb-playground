package handler

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/reservation"
	"github.com/redis/go-redis/v9"
)

func HandleReservationStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, redisClient *redis.Client) error {
	handler := func(event esdb.RecordedEvent) error {
		var res reservation.Reservation
		if err := json.Unmarshal(event.Data, &res); err != nil {
			logger.Error("failed to unmarshal a reservation event", "event", event, "error", err)
		}

		if _, err := reservation.PersistReservation(ctx, redisClient, res); err != nil {
			logger.Error("failed to persist a reservation", "event", event, "error", err)
		}

		return nil
	}

	return db.HandleStream(ctx, esdbClient, string(events.ReservationStream), handler)
}
