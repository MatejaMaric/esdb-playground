package projections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/projections-playground/events"
	"github.com/gofrs/uuid"
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

func (p *streamProjection) HandleEvent(resolved *esdb.ResolvedEvent) error {
	if resolved.Event == nil {
		return errors.New("resolved.Event is nil")
	}

	switch resolved.Event.EventType {
	case string(events.CreateUser):
		return p.handleCreateUserEvent(resolved.Event)
	default:
		return fmt.Errorf("unknown event type: %s", resolved.Event.EventType)
	}
}

func (p *streamProjection) handleCreateUserEvent(rawEvent *esdb.RecordedEvent) error {
	var event events.CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user := events.UserStateEvent{
		Username:   event.Username,
		LoginCount: 0,
		Version:    0,
	}

	streamName := events.UserStateStream.ForUser(event.Username)

	eventId, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to create a uuid: %w", err)
	}

	stateEventData, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}

	stateEvent := esdb.EventData{
		EventID:     eventId,
		EventType:   string(events.UserState),
		ContentType: esdb.JsonContentType,
		Data:        stateEventData,
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

func (p *streamProjection) handleLoginUserEvent(rawEvent *esdb.RecordedEvent) error {
	var event events.LoginUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
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
