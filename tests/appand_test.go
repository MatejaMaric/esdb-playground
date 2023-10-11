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

	event := esdb.EventData{
		EventID:     uuid.Must(uuid.NewV4()),
		EventType:   "someEventType",
		ContentType: esdb.BinaryContentType,
		Data:        []byte{0x00, 0x01, 0x02, 0x03},
	}

	wr, err := TestEsdbClient.AppendToStream(ctx, "stream-A", esdb.AppendToStreamOptions{}, event)
	if err != nil {
		t.Fatal(err)
	}

	data, _ := json.MarshalIndent(wr, "", "  ")

	t.Log(string(data))
}
