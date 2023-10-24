package tests

import (
	"context"
	"testing"
	"time"

	"github.com/MatejaMaric/esdb-playground/reservation"
	"github.com/redis/go-redis/v9"
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

	ttl := CheckTTL(t, ctx, TestRedisClient, res.Key)
	if ttl != -1 {
		t.Fatal("persisted reservation should not have an expiration (TTL)")
	}

	var getCmd *redis.StringCmd = TestRedisClient.Get(ctx, res.Key)
	if err := getCmd.Err(); err != nil {
		t.Log(err)
	}

	if getCmd.Val() != "persisted" {
		t.Fatalf("unexpected token: %s", getCmd.Val())
	}
}
