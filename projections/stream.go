package projections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/gofrs/uuid"
)

type streamProjection struct {
	ctx        context.Context
	esdbClient *esdb.Client
}

func NewStreamProjection(ctx context.Context, esdbClient *esdb.Client) (Projection, error) {
	md := esdb.StreamMetadata{}
	md.SetMaxCount(16)

	_, err := esdbClient.SetStreamMetadata(ctx, events.UserStateStream, esdb.AppendToStreamOptions{}, md)
	if err != nil {
		return nil, fmt.Errorf("error when setting stream metadata: %w", err)
	}

	return &streamProjection{
		ctx:        ctx,
		esdbClient: esdbClient,
	}, nil
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

	_, err = p.esdbClient.AppendToStream(p.ctx, events.UserStateStream, aopts, stateEvent)
	if err != nil {
		return fmt.Errorf("failed to append to stream: %w", err)
	}

	return nil
}

func (p *streamProjection) handleLoginUserEvent(event events.LoginUserEvent) error {
	return nil
}
