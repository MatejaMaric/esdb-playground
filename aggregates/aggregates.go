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

func NewUserAggregate(ctx context.Context, esdbClient *esdb.Client, username string) (UserAggregate, error) {
	streamName := events.UserEventsStream.ForUser(username)

	user := UserAggregate{}
	var err error

	handler := func(re esdb.RecordedEvent) error {
		user, err = user.Apply(re)
		return err
	}

	if err := db.HandleReadStream(ctx, esdbClient, streamName, handler); err != nil {
		return user, err
	}

	return user, nil
}

func (ua UserAggregate) Apply(re esdb.RecordedEvent) (UserAggregate, error) {
	switch re.EventType {
	case string(events.CreateUser):
		return ua.ApplyCreateUser(re)
	case string(events.LoginUser):
		return ua.ApplyLoginUser(re)
	default:
		return ua, fmt.Errorf("unknown event type: %v", re.EventType)
	}
}

func (ua UserAggregate) ApplyCreateUser(re esdb.RecordedEvent) (UserAggregate, error) {
	if _, err := ua.IsEventNumberExpected(re); err != nil {
		return ua, err
	}

	var event events.CreateUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return UserAggregate{
		Username:   event.Username,
		Email:      event.Email,
		LoginCount: 0,
		Version:    0,
	}, nil
}

func (ua UserAggregate) ApplyLoginUser(re esdb.RecordedEvent) (UserAggregate, error) {
	if _, err := ua.IsEventNumberExpected(re); err != nil {
		return ua, err
	}

	var event events.LoginUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return UserAggregate{
		Username:   ua.Username,
		Email:      ua.Email,
		LoginCount: ua.LoginCount + 1,
		Version:    ua.Version + 1,
	}, nil
}

func (ua UserAggregate) IsEventNumberExpected(re esdb.RecordedEvent) (bool, error) {
	var expectedVersion uint64
	if re.EventNumber == 0 {
		expectedVersion = 0
	} else {
		expectedVersion = re.EventNumber - 1
	}

	if ua.Version != expectedVersion {
		return false, fmt.Errorf("unexpected Event Number %v, wanted %v", re.EventNumber, ua.Version+1)
	}

	return true, nil
}
