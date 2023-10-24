package reservation

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

const Timeout time.Duration = 3 * time.Second

type Reservation struct {
	Key         string
	AccessToken string
}

func CreateReservation(ctx context.Context, redisClient *redis.Client, value string) (Reservation, error) {
	token, err := uuid.NewV4()
	if err != nil {
		return Reservation{}, fmt.Errorf("failed creating an uuid: %w", err)
	}

	if err := redisClient.SetNX(ctx, value, token.String(), Timeout).Err(); err != nil {
		return Reservation{}, fmt.Errorf("failed to reserve in Redis: %w", err)
	}

	return Reservation{
		Key:         value,
		AccessToken: token.String(),
	}, nil
}

func PresistReservation(ctx context.Context, redisClient *redis.Client, reservation Reservation) (Reservation, error) {
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
