package projections

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/MatejaMaric/esdb-playground/aggregates"
	"github.com/MatejaMaric/esdb-playground/events"
)

type streamProjection struct {
	ctx        context.Context
	esdbClient *esdb.Client
}

func NewStreamProjection(ctx context.Context, esdbClient *esdb.Client) Projection {
	return &streamProjection{
		ctx:        ctx,
		esdbClient: esdbClient,
	}
}

func (p *streamProjection) HandleEvent(event esdb.RecordedEvent) error {
	switch event.EventType {
	case string(events.CreateUser):
		return p.handleCreateUserEvent(event)
	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}

func (p *streamProjection) handleCreateUserEvent(re esdb.RecordedEvent) error {
	var event events.CreateUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user, err := aggregates.User{}.ApplyCreateUser(re)
	if err != nil {
		return err
	}

	streamName := events.UserStateStream.ForUser(event.Username)

	stateEvent, err := events.Create(events.UserAggregate, user)
	if err != nil {
		return err
	}

	aopts := esdb.AppendToStreamOptions{ExpectedRevision: esdb.NoStream{}}

	_, err = p.esdbClient.AppendToStream(p.ctx, streamName, aopts, stateEvent)
	if err != nil {
		return fmt.Errorf("failed to append to stream: %w", err)
	}

	smd := esdb.StreamMetadata{}
	smd.SetMaxCount(16)

	_, err = p.esdbClient.SetStreamMetadata(p.ctx, streamName, esdb.AppendToStreamOptions{}, smd)
	if err != nil {
		return fmt.Errorf("error when setting stream metadata: %w", err)
	}

	return nil
}

func (p *streamProjection) handleLoginUserEvent(re esdb.RecordedEvent) error {
	var event events.LoginUserEvent
	if err := json.Unmarshal(re.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return nil
}

func getLatestEvent(ctx context.Context, esdbClient *esdb.Client, streamName string) (*esdb.ResolvedEvent, error) {
	ropts := esdb.ReadStreamOptions{
		Direction: esdb.Backwards,
		From:      esdb.End{},
	}

	rs, err := esdbClient.ReadStream(ctx, streamName, ropts, 1)
	if err != nil {
		return nil, fmt.Errorf("failed reading stream %s: %w", streamName, err)
	}
	defer rs.Close()

	receivedEvent, err := rs.Recv()
	if err != nil {
		return nil, fmt.Errorf("ReadStream.Recv(): %w", err)
	}

	return receivedEvent, nil
}
