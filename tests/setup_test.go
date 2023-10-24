package tests

import (
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
)

func TestMain(m *testing.M) {
	log.Default().Print("Running TestMain")
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resources := CreateResources(pool, map[string]SpawnFunc{
		"EventStoreDB": SpawnTestEventStoreDB,
		"Redis":        SpawnTestRedis,
		// "MariaDB":      SpawnTestMariaDB,
	})

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	PurgeResources(pool, resources)

	os.Exit(code)
}
