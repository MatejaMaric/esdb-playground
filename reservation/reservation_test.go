package reservation_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/reservation"
	"github.com/MatejaMaric/esdb-playground/tests"
	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var (
	TestEsdbClient  *esdb.Client
	TestRedisClient *redis.Client
	TestReservation reservation.Reservation
)

func TestMain(m *testing.M) {
	pool := tests.SetupDockertestPool()

	var resourceRedis, resourceEventStoreDB *dockertest.Resource

	eg := &errgroup.Group{}

	eg.Go(func() error {
		var err error
		TestRedisClient, resourceRedis, err = tests.SpawnTestRedis(pool)
		return err
	})

	eg.Go(func() error {
		var err error
		TestEsdbClient, resourceEventStoreDB, err = tests.SpawnTestEventStoreDB(pool)
		return err
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	tests.PurgeResources(pool, resourceRedis, resourceEventStoreDB)

	os.Exit(code)
}

func TestCreateReservation(t *testing.T) {
	ctx := context.Background()
	var err error

	TestReservation, err = reservation.CreateReservation(ctx, TestRedisClient, "unique@email.com")
	if err != nil {
		t.Fatal(err)
	}

	CheckTTL(t, ctx, TestRedisClient, TestReservation.Key)

	time.Sleep(1 * time.Second)

	CheckTTL(t, ctx, TestRedisClient, TestReservation.Key)

	_, err = reservation.CreateReservation(ctx, TestRedisClient, "unique@email.com")
	if err == nil {
		t.Fatal("error expected when making a duplicate reservation!")
	}
}

func TestSaveReservation(t *testing.T) {
	ctx := context.Background()

	_, err := reservation.SaveReservation(ctx, TestEsdbClient, TestReservation)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPersistReservation(t *testing.T) {
	ctx := context.Background()

	res, err := reservation.PersistReservation(ctx, TestRedisClient, TestReservation)
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
