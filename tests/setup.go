package tests

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
)

func SetupDockertestPool() *dockertest.Pool {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	if err := pool.Client.Ping(); err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	return pool
}

func PurgeResources(pool *dockertest.Pool, resources ...*dockertest.Resource) {
	for i, resource := range resources {
		if resource == nil {
			log.Printf("dockertest resource provided at index %d is nil", i)
			continue
		}
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}

func SpawnTestMariaDB(pool *dockertest.Pool) (*sql.DB, *dockertest.Resource, error) {
	ropts := dockertest.RunOptions{
		Repository: "mariadb",
		Tag:        "11.0.3-jammy",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
	}

	resource, err := pool.RunWithOptions(&ropts)
	if err != nil {
		return nil, nil, err
	}

	var sqlClient *sql.DB

	retry := func() error {
		var err error
		sqlClient, err = sql.Open("mysql", fmt.Sprintf("root:secret@(localhost:%s)/mysql", resource.GetPort("3306/tcp")))
		if err != nil {
			return err
		}
		return sqlClient.Ping()
	}

	if err := pool.Retry(retry); err != nil {
		return nil, nil, err
	}

	return sqlClient, resource, nil
}

func SpawnTestEventStoreDB(pool *dockertest.Pool) (*esdb.Client, *dockertest.Resource, error) {
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

	var esdbClient *esdb.Client

	retry := func() error {
		if resource != nil && resource.Container != nil {
			containerInfo, containerError := pool.Client.InspectContainer(resource.Container.ID)
			if containerError == nil && !containerInfo.State.Running {
				return fmt.Errorf("unexpected exit of container check the container logs for more information, container ID: %v", resource.Container.ID)
			}
		}

		healthCheckEndpoint := fmt.Sprintf("http://localhost:%s/health/alive", resource.GetPort("2113/tcp"))
		_, err := http.Get(healthCheckEndpoint)
		if err != nil {
			return err
		}

		esdbClient, err = esdb.NewClient(esdbConf)
		return err
	}

	if err := pool.Retry(retry); err != nil {
		return nil, nil, err
	}

	return esdbClient, resource, nil
}

func SpawnTestRedis(pool *dockertest.Pool) (*redis.Client, *dockertest.Resource, error) {
	ropts := dockertest.RunOptions{
		Repository: "redis",
		Tag:        "7.2-alpine3.18",
	}

	resource, err := pool.RunWithOptions(&ropts)
	if err != nil {
		return nil, nil, err
	}

	var redisClient *redis.Client

	retry := func() error {
		redisClient = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})
		return redisClient.Ping(context.Background()).Err()
	}

	if err := pool.Retry(retry); err != nil {
		return nil, nil, err
	}

	return redisClient, resource, nil
}
