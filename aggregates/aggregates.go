package aggregates

import (
	"encoding/json"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
)

type User struct {
	Username   string `json:"username"`
	Email      string `json:"email"`
	LoginCount int32  `json:"login_count"`
	Version    uint64 `json:"version"`
}

func (ua User) Apply(re esdb.RecordedEvent) (User, error) {
	switch re.EventType {
	case string(events.CreateUser):
		return ua.ApplyCreateUser(re)
	case string(events.LoginUser):
		return ua.ApplyLoginUser(re)
	default:
		return ua, fmt.Errorf("unknown event type: %v", re.EventType)
	}
}

func (ua User) ApplyCreateUser(re esdb.RecordedEvent) (User, error) {
	if _, err := ua.IsEventNumberExpected(re); err != nil {
		return ua, err
	}

	var event events.CreateUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return User{
		Username:   event.Username,
		Email:      event.Email,
		LoginCount: 0,
		Version:    0,
	}, nil
}

func (ua User) ApplyLoginUser(re esdb.RecordedEvent) (User, error) {
	if _, err := ua.IsEventNumberExpected(re); err != nil {
		return ua, err
	}

	var event events.LoginUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return ua, fmt.Errorf("failed to json unmarshal event data: %w", err)
	}

	return User{
		Username:   ua.Username,
		Email:      ua.Email,
		LoginCount: ua.LoginCount + 1,
		Version:    ua.Version + 1,
	}, nil
}

func (ua User) IsEventNumberExpected(re esdb.RecordedEvent) (bool, error) {
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
