package projections

import "github.com/EventStore/EventStore-Client-Go/v3/esdb"

type Projection interface {
	HandleEvent(esdb.RecordedEvent) error
}
