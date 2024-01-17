package utils

import (
	"encoding/json"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/gofrs/uuid"
)

type FakeEvent struct {
	Type events.Event
	Data any
}

func FakeRecordedEvents(streamId string, events []FakeEvent) []esdb.RecordedEvent {
	var res []esdb.RecordedEvent

	for idx, fe := range events {
		jsonData, err := json.Marshal(fe.Data)
		if err != nil {
			panic(err)
		}

		re := esdb.RecordedEvent{
			EventID:        uuid.Must(uuid.NewV4()),
			EventType:      string(fe.Type),
			ContentType:    "application/json",
			StreamID:       streamId,
			EventNumber:    uint64(idx),
			Position:       esdb.Position{},
			CreatedDate:    time.Now(),
			Data:           jsonData,
			SystemMetadata: map[string]string{},
			UserMetadata:   []byte{},
		}

		res = append(res, re)
	}

	return res
}
