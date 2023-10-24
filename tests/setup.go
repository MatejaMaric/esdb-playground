package tests

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
)

var (
	TestDb          *sql.DB
	TestEsdbClient  *esdb.Client
	TestRedisClient *redis.Client
)

type SpawnFunc func(pool *dockertest.Pool) (*dockertest.Resource, func() error, error)

func CreateResources(pool *dockertest.Pool, resourceFuncs map[string]SpawnFunc) []*dockertest.Resource {
	var resources []*dockertest.Resource

	for name, spawn := range resourceFuncs {
		resource, retry, err := spawn(pool)
		if err != nil {
			log.Fatalf("could not start %s: %v", name, err)
		}

		if err := pool.Retry(retry); err != nil {
			log.Fatalf("could not connect to %s: %v", name, err)
		}

		resources = append(resources, resource)
	}

	return resources
}

func PurgeResources(pool *dockertest.Pool, resources []*dockertest.Resource) {
	for _, resource := range resources {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}

func SpawnTestMariaDB(pool *dockertest.Pool) (*dockertest.Resource, func() error, error) {
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

func SpawnTestEventStoreDB(pool *dockertest.Pool) (*dockertest.Resource, func() error, error) {
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
			if containerError == nil && !containerInfo.State.Running {
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

func SpawnTestRedis(pool *dockertest.Pool) (*dockertest.Resource, func() error, error) {
	ropts := dockertest.RunOptions{
		Repository: "redis",
		Tag:        "7.2-alpine3.18",
	}

	resource, err := pool.RunWithOptions(&ropts)
	if err != nil {
		return nil, nil, err
	}

	retryFunc := func() error {
		TestRedisClient = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})
		return TestRedisClient.Ping(context.Background()).Err()
	}

	return resource, retryFunc, nil
}

func CheckTTL(t *testing.T, ctx context.Context, redisClient *redis.Client, key string) {
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
}
