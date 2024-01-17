package reservation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
)

const Timeout time.Duration = 3 * time.Second

var ErrReservationExists = errors.New("reservation already exists for the key")

type Reservation struct {
	Key         string
	AccessToken string
}

/*
Create a reservation and store it inside Redis with a three second TTL
*/
func CreateReservation(ctx context.Context, redisClient *redis.Client, value string) (Reservation, error) {
	token, err := uuid.NewV4()
	if err != nil {
		return Reservation{}, fmt.Errorf("failed creating an uuid: %w", err)
	}

	ok, err := redisClient.SetNX(ctx, value, token.String(), Timeout).Result()
	if err != nil {
		return Reservation{}, fmt.Errorf("failed to reserve in Redis: %w", err)
	}
	if !ok {
		return Reservation{}, ErrReservationExists
	}

	return Reservation{
		Key:         value,
		AccessToken: token.String(),
	}, nil
}

/*
Write a reservation into EventStoreDB reservation stream.

After the reservation was written, subscription to reservation stream should call the PresistReservation function.
*/
func SaveReservation(ctx context.Context, esdbClient *esdb.Client, reservation Reservation) (*esdb.WriteResult, error) {
	return db.AppendEvent(ctx, esdbClient, string(events.ReservationStream), events.ReserveEmail, reservation, esdb.Any{})
}

/*
Set reservation to never expire inside Redis.
*/
func PersistReservation(ctx context.Context, redisClient *redis.Client, reservation Reservation) (Reservation, error) {
	const redisLuaScript string = `if redis.call('GET',KEYS[1]) == ARGV[1]
then
    return redis.call('SET',KEYS[1],'persisted')
else
    return 0
end`

	res := redisClient.Eval(ctx, redisLuaScript, []string{reservation.Key}, reservation.AccessToken)
	if err := res.Err(); err != nil {
		return Reservation{}, fmt.Errorf("failed to persist the reservation: %w", err)
	}

	return Reservation{
		Key:         reservation.Key,
		AccessToken: "persisted",
	}, nil
}

func RepopulateRedis(ctx context.Context, esdbClient *esdb.Client, redisClient *redis.Client) error {
	handler := func(event esdb.RecordedEvent) error {
		var reservation Reservation
		if err := json.Unmarshal(event.Data, &reservation); err != nil {
			return fmt.Errorf("failed to unmarshal the reservation: %w", err)
		}

		_, err := PersistReservation(ctx, redisClient, reservation)
		return err
	}

	err := db.HandleReadStream(ctx, esdbClient, string(events.ReservationStream), handler)
	if esdbErr, isNil := esdb.FromError(err); !isNil && esdbErr.Code() == esdb.ErrorCodeResourceNotFound {
		return nil
	}

	return err
}
