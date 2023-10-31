package aggregates_test

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/aggregates"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/tests"
	"github.com/go-test/deep"
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

func TestNewUserAggregate(t *testing.T) {
	ctx := context.Background()

	eds := []esdb.EventData{
		events.MustCreate(events.CreateUser, events.CreateUserEvent{"test", "test@test.com"}),
		events.MustCreate(events.LoginUser, events.LoginUserEvent{"test"}),
		events.MustCreate(events.LoginUser, events.LoginUserEvent{"test"}),
	}

	_, err := TestEsdbClient.AppendToStream(ctx, events.UserEventsStream.ForUser("test"), esdb.AppendToStreamOptions{}, eds...)
	if err != nil {
		t.Fatal(err)
	}

	ua, err := aggregates.NewUserAggregate(ctx, TestEsdbClient, "test")
	if err != nil {
		t.Fatal(err)
	}

	expectedUa := aggregates.UserAggregate{"test", "test@test.com", 2, 2}

	if diff := deep.Equal(expectedUa, ua); diff != nil {
		t.Fatalf("unexpected user aggregate:\n%v\n", strings.Join(diff, "\n"))
	}
}