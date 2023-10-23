package aggregates

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
)

type UserAggregate struct {
	Username   string `json:"username"`
	Email      string `json:"email"`
	LoginCount int32  `json:"login_count"`
	Version    uint64 `json:"version"`
}

func NewUserAggregate(ctx context.Context, esdbClient *esdb.Client, username string) (*UserAggregate, error) {
	streamName := events.UserEventsStream.ForUser(username)

	fold := func(ua UserAggregate, re esdb.RecordedEvent) (UserAggregate, error) {
		return ua.Apply(re)
	}

	return db.AggregateStream[UserAggregate](ctx, esdbClient, streamName, fold)
}

func (ua UserAggregate) Apply(event esdb.RecordedEvent) (UserAggregate, error) {
	var expectedVersion uint64
	if event.EventNumber == 0 {
		expectedVersion = 0
	} else {
		expectedVersion = event.EventNumber - 1
	}

	if ua.Version != expectedVersion {
		return ua, fmt.Errorf("unexpected Event Number %v, wanted %v", event.EventNumber, ua.Version+1)
	}

	switch event.EventType {
	case string(events.CreateUser):
		return ua.applyCreateUser(event.Data)
	case string(events.LoginUser):
		return ua.applyLoginUser(event.Data)
	default:
		return ua, fmt.Errorf("unknown event type: %v", event.EventType)
	}
}

func (ua UserAggregate) applyCreateUser(eventData []byte) (UserAggregate, error) {
	var event events.CreateUserEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return UserAggregate{
		Username:   event.Username,
		Email:      event.Email,
		LoginCount: 0,
		Version:    0,
	}, nil
}

func (ua UserAggregate) applyLoginUser(eventData []byte) (UserAggregate, error) {
	var event events.LoginUserEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return UserAggregate{
		Username:   ua.Username,
		Email:      ua.Email,
		LoginCount: ua.LoginCount + 1,
		Version:    ua.Version + 1,
	}, nil
}
