package tests

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
)

var (
	TestDb         *sql.DB
	TestEsdbClient *esdb.Client
)

func spawnTestMariaDB(pool *dockertest.Pool) (*dockertest.Resource, func() error, error) {
	ropts := dockertest.RunOptions{
		Repository: "mariadb",
		Tag:        "11.0.3-jammy",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
	}

	resource, err := pool.RunWithOptions(&ropts)
	if err != nil {
		return nil, nil, err
	}

	retryFunc := func() error {
		var err error
		TestDb, err = sql.Open("mysql", fmt.Sprintf("root:secret@(localhost:%s)/mysql", resource.GetPort("3306/tcp")))
		if err != nil {
			return err
		}
		return TestDb.Ping()
	}

	return resource, retryFunc, nil
}

func spawnTestEventStoreDB(pool *dockertest.Pool) (*dockertest.Resource, func() error, error) {
	ropts := dockertest.RunOptions{
		Repository:   "eventstore/eventstore",
		Tag:          "22.10.3-alpha-arm64v8",
		ExposedPorts: []string{"1113", "2113"},
		Env: []string{
			"EVENTSTORE_CLUSTER_SIZE=1",
			"EVENTSTORE_RUN_PROJECTIONS=All",
			"EVENTSTORE_START_STANDARD_PROJECTIONS=true",
			"EVENTSTORE_EXT_TCP_PORT=1113",
			"EVENTSTORE_HTTP_PORT=2113",
			"EVENTSTORE_INSECURE=true",
			"EVENTSTORE_ENABLE_EXTERNAL_TCP=true",
			"EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true",
		},
	}

	resource, err := pool.RunWithOptions(&ropts)
	if err != nil {
		return nil, nil, err
	}

	connectionStr := fmt.Sprintf("esdb://localhost:%s?tls=false", resource.GetPort("2113/tcp"))
	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse EventStoreDB connection string: %w", err)
	}

	retryFunc := func() error {
		if resource != nil && resource.Container != nil {
			containerInfo, containerError := pool.Client.InspectContainer(resource.Container.ID)
			if containerError == nil && containerInfo.State.Running == false {
				return fmt.Errorf("unexpected exit of container check the container logs for more information, container ID: %v", resource.Container.ID)
			}
		}

		healthCheckEndpoint := fmt.Sprintf("http://localhost:%s/health/alive", resource.GetPort("2113/tcp"))
		_, err := http.Get(healthCheckEndpoint)
		if err != nil {
			return err
		}

		TestEsdbClient, err = esdb.NewClient(esdbConf)
		return err
	}

	return resource, retryFunc, nil
}

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// dbResource, dbRetryFunc, err := spawnTestMariaDB(pool)
	// if err != nil {
	// 	log.Fatalf("could not start MariaDB: %s", err)
	// }

	esdbResource, esdbRetryFunc, err := spawnTestEventStoreDB(pool)
	if err != nil {
		log.Fatalf("could not start EventStoreDB: %s", err)
	}

	// if err := pool.Retry(dbRetryFunc); err != nil {
	// 	log.Fatalf("Could not connect to MariaDB: %s", err)
	// }

	if err := pool.Retry(esdbRetryFunc); err != nil {
		log.Fatalf("Could not connect to EventStoreDB: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	// if err := pool.Purge(dbResource); err != nil {
	// 	log.Fatalf("Could not purge resource: %s", err)
	// }

	if err := pool.Purge(esdbResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}
