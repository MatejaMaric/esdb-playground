package aggregate

import (
	"github.com/MatejaMaric/esdb-playground/reservation-pattern/events"
)

type UserAggregate struct {
	events.UserStateEvent
}

func (ua UserAggregate) Apply(eventType events.Event, eventData any) UserAggregate {
	return UserAggregate{}
}
