package aggregates

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
)

type UserAggregate struct {
	Username   string `json:"username"`
	Email      string `json:"email"`
	LoginCount int32  `json:"login_count"`
	Version    uint64 `json:"version"`
}

func NewUserAggregate(ctx context.Context, esdbClient *esdb.Client, username string) (UserAggregate, error) {
	streamName := events.UserEventsStream.ForUser(username)

	ropts := esdb.ReadStreamOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}

	stream, err := esdbClient.ReadStream(ctx, streamName, ropts, math.MaxUint64)
	if err != nil {
		return UserAggregate{}, fmt.Errorf("failed to read the stream '%s': %w", streamName, err)
	}
	defer stream.Close()

	user := UserAggregate{}

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return user, fmt.Errorf("error while reading events from the stream %s: %w", streamName, err)
		}

		if event.Event == nil {
			return user, fmt.Errorf("event is nil!")
		}

		user, err = user.Apply(event.Event)
		if err != nil {
			return user, fmt.Errorf("applying the event returned an error: %w", err)
		}
	}

	return user, nil
}

func (ua UserAggregate) Apply(event *esdb.RecordedEvent) (UserAggregate, error) {
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
