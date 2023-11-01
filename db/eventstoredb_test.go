package db_test

import (
	"context"
	"strings"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/aggregates"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/go-test/deep"
)

func TestNewUserFromStream(t *testing.T) {
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

	ua, err := db.NewUserFromStream(ctx, TestEsdbClient, "test")
	if err != nil {
		t.Fatal(err)
	}

	expectedUa := aggregates.User{"test", "test@test.com", 2, 2}

	if diff := deep.Equal(expectedUa, ua); diff != nil {
		t.Fatalf("unexpected user aggregate:\n%v\n", strings.Join(diff, "\n"))
	}
}
