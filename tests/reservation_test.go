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

	_, err = reservation.CreateReservation(ctx, TestRedisClient, "unique@email.com")
	if err == nil {
		t.Fatal("error expected when making a duplicate reservation!")
	}

	_, err = reservation.SaveReservation(ctx, TestEsdbClient, res)

	res, err = reservation.PersistReservation(ctx, TestRedisClient, res)
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

func CheckTTL(t *testing.T, ctx context.Context, redisClient *redis.Client, key string) time.Duration {
	ttlCmd := TestRedisClient.TTL(ctx, key)
	if err := ttlCmd.Err(); err != nil {
		t.Fatal(err)
	}

	ttl := ttlCmd.Val()

	switch ttl {
	case -2:
		t.Log("TTL is -2 (key does not exist)")
	case -1:
		t.Log("TTL is -1 (key exists, but has no associated expiry)")
	default:
		t.Logf("TTL: %s", ttl)
	}

	return ttl
}
