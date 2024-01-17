package db_test

import (
	"log"
	"os"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/tests"
	"github.com/ory/dockertest/v3"
)

var TestEsdbClient *esdb.Client

func TestMain(m *testing.M) {
	pool := tests.SetupDockertestPool()

	var resourceEventStoreDB *dockertest.Resource
	var err error

	TestEsdbClient, resourceEventStoreDB, err = tests.SpawnTestEventStoreDB(pool)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	tests.PurgeResources(pool, resourceEventStoreDB)

	os.Exit(code)
}
