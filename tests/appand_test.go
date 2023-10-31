package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
)

func TestAppand(t *testing.T) {
	ctx := context.Background()

	wr1, err := TestEsdbClient.AppendToStream(ctx, "user-A", esdb.AppendToStreamOptions{}, events.MustCreate(events.CreateUser, nil))
	if err != nil {
		t.Fatalf("failed appending 1. event to user-A: %v", err)
	}
	LogWriteResult(t, wr1)

	wr2, err := TestEsdbClient.AppendToStream(ctx, "random-A", esdb.AppendToStreamOptions{}, events.MustCreate(events.CreateUser, nil))
	if err != nil {
		t.Fatalf("failed appending 1. event to random-A: %v", err)
	}
	LogWriteResult(t, wr2)

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(wr1.NextExpectedVersion),
	}

	wr3, err := TestEsdbClient.AppendToStream(ctx, "user-A", aopts, events.MustCreate(events.CreateUser, nil))
	if err != nil {
		t.Fatalf("failed appending 2. event to user-A: %v", err)
	}
	LogWriteResult(t, wr3)
}

func LogWriteResult(t *testing.T, wr *esdb.WriteResult) {
	data, err := json.MarshalIndent(wr, "", "  ")
	if err != nil {
		t.Errorf("failed to log write result: %v", err)
	}

	t.Log(string(data))
}
