package events

import (
	"encoding/json"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/gofrs/uuid"
)

type Stream string

const (
	UserEventsStream  Stream = "user_events"
	UserStateStream   Stream = "user_state"
	ReservationStream Stream = "reservations"
)

func (s Stream) ForUser(username string) string {
	return fmt.Sprintf("%s-%s", s, username)
}

type Event string

const (
	UserAggregate Event = "UserAggregate"
	CreateUser    Event = "CreateUser"
	LoginUser     Event = "LoginUser"
	ReserveEmail  Event = "ReserveEmail"
)

type CreateUserEvent struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

type LoginUserEvent struct {
	Username string `json:"username"`
}

func Create(eventType Event, eventData any) (esdb.EventData, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return esdb.EventData{}, fmt.Errorf("failed to create a uuid: %w", err)
	}

	jsonData, err := json.Marshal(eventData)
	if err != nil {
		return esdb.EventData{}, fmt.Errorf("failed to marshal json: %w", err)
	}

	return esdb.EventData{
		EventID:     eventId,
		EventType:   string(eventType),
		ContentType: esdb.ContentTypeJson,
		Data:        jsonData,
	}, nil
}

func MustCreate(eventType Event, eventData any) esdb.EventData {
	jsonData, err := json.Marshal(eventData)
	if err != nil {
		panic(fmt.Errorf("failed to marshal json: %w", err))
	}

	return esdb.EventData{
		EventID:     uuid.Must(uuid.NewV4()),
		EventType:   string(eventType),
		ContentType: esdb.ContentTypeJson,
		Data:        jsonData,
	}
}
