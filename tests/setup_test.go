package tests

import (
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var (
	TestEsdbClient  *esdb.Client
	TestRedisClient *redis.Client
	TestSqlClient   *sql.DB
)

func TestMain(m *testing.M) {
	pool := SetupDockertestPool()

	var resourceMariaDB, resourceRedis, resourceEventStoreDB *dockertest.Resource

	eg := &errgroup.Group{}

	/*
		eg.Go(func() error {
			var err error
			TestSqlClient, resourceMariaDB, err = SpawnTestMariaDB(pool)
			return err
		})

		eg.Go(func() error {
			var err error
			TestRedisClient, resourceRedis, err = SpawnTestRedis(pool)
			return err
		})
	*/

	eg.Go(func() error {
		var err error
		TestEsdbClient, resourceEventStoreDB, err = SpawnTestEventStoreDB(pool)
		return err
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	PurgeResources(pool, resourceMariaDB, resourceRedis, resourceEventStoreDB)

	os.Exit(code)
}
