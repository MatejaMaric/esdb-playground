package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
)

func TestAppand(t *testing.T) {
	ctx := context.Background()

	wr1, err := TestEsdbClient.AppendToStream(ctx, "user-A", esdb.AppendToStreamOptions{}, genRandomEvent())
	if err != nil {
		t.Fatalf("failed appending 1. event to user-A: %v", err)
	}
	logWriteResult(t, wr1)

	wr2, err := TestEsdbClient.AppendToStream(ctx, "random-A", esdb.AppendToStreamOptions{}, genRandomEvent())
	if err != nil {
		t.Fatalf("failed appending 1. event to random-A: %v", err)
	}
	logWriteResult(t, wr2)

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(wr1.NextExpectedVersion),
	}

	wr3, err := TestEsdbClient.AppendToStream(ctx, "user-A", aopts, genRandomEvent())
	if err != nil {
		t.Fatalf("failed appending 2. event to user-A: %v", err)
	}
	logWriteResult(t, wr3)
}

func genRandomEvent() esdb.EventData {
	return esdb.EventData{
		EventID:     uuid.Must(uuid.NewV4()),
		EventType:   "someEventType",
		ContentType: esdb.BinaryContentType,
		Data:        []byte{0x00, 0x01, 0x02, 0x03},
	}
}

func logWriteResult(t *testing.T, wr *esdb.WriteResult) {
	data, err := json.MarshalIndent(wr, "", "  ")
	if err != nil {
		t.Errorf("failed to log write result: %v", err)
	}

	t.Log(string(data))
}
