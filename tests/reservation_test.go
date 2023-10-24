package tests

import (
	"context"
	"testing"
	"time"

	"github.com/MatejaMaric/esdb-playground/reservation"
)

func TestReservationLogic(t *testing.T) {
	ctx := context.Background()

	res, err := reservation.CreateReservation(ctx, TestRedisClient, "unique@email.com")
	if err != nil {
		t.Fatal(err)
	}

	CheckTTL(t, ctx, TestRedisClient, res.Key)

	time.Sleep(2 * time.Second)

	CheckTTL(t, ctx, TestRedisClient, res.Key)

	res, err = reservation.PresistReservation(ctx, TestRedisClient, res)
	if err != nil {
		t.Fatal(err)
	}

	CheckTTL(t, ctx, TestRedisClient, res.Key)
}
