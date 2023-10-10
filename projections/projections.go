package projections

import "github.com/EventStore/EventStore-Client-Go/esdb"

type Projection interface {
	HandleEvent(*esdb.ResolvedEvent) error
}
