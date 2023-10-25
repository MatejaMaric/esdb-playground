package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
)

func ConnectToEventStoreDB() (*esdb.Client, error) {
	const connectionStr string = "esdb://localhost:2113?tls=false"

	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EventStoreDB connection string: %w", err)
	}

	return esdb.NewClient(esdbConf)
}

func AppendEvent(
	ctx context.Context,
	esdbClient *esdb.Client,
	streamName string,
	eventType string,
	eventData any,
	expectedRevision esdb.ExpectedRevision,
) (*esdb.WriteResult, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to create a uuid: %w", err)
	}

	jsonData, err := json.Marshal(eventData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}

	esdbEvent := esdb.EventData{
		EventID:     eventId,
		EventType:   eventType,
		ContentType: esdb.JsonContentType,
		Data:        jsonData,
	}

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: expectedRevision,
	}

	appendResult, err := esdbClient.AppendToStream(ctx, streamName, aopts, esdbEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to append to stream: %w", err)
	}

	return appendResult, nil
}

func HandleReadStream(ctx context.Context, esdbClient *esdb.Client, streamName string, handler func(esdb.RecordedEvent) error) error {
	ropts := esdb.ReadStreamOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}

	stream, err := esdbClient.ReadStream(ctx, streamName, ropts, math.MaxUint64)
	if err != nil {
		return fmt.Errorf("failed to read the stream '%s': %w", streamName, err)
	}
	defer stream.Close()

	for {
		resolved, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("error while reading events from the stream %s: %w", streamName, err)
		}

		if resolved.Event == nil {
			return fmt.Errorf("event is nil!")
		}

		err = handler(*resolved.Event)
		if err != nil {
			return fmt.Errorf("the event handler returned an error: %w", err)
		}
	}

	return nil
}

func HandleAllStream(ctx context.Context, esdbClient *esdb.Client, opts esdb.SubscribeToAllOptions, handler func(esdb.RecordedEvent) error) error {
	stream, err := esdbClient.SubscribeToAll(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %w", err)
	}

	if err := HandleSubscription(stream, handler); err != nil {
		return err
	}

	if err := stream.Close(); err != nil {
		return fmt.Errorf("closing the stream resulted in an error: %w", err)
	}

	return nil
}

func HandleStream(ctx context.Context, esdbClient *esdb.Client, streamName string, handler func(esdb.RecordedEvent) error) error {
	stream, err := esdbClient.SubscribeToStream(ctx, streamName, esdb.SubscribeToStreamOptions{})
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream %s: %w", streamName, err)
	}

	if err := HandleSubscription(stream, handler); err != nil {
		return err
	}

	if err := stream.Close(); err != nil {
		return fmt.Errorf("closing the stream resulted in an error: %w", err)
	}

	return nil
}

func HandleSubscription(stream *esdb.Subscription, handler func(esdb.RecordedEvent) error) error {
	for {
		var subEvent *esdb.SubscriptionEvent = stream.Recv()

		if subEvent.EventAppeared != nil {
			var resolved *esdb.ResolvedEvent = subEvent.EventAppeared

			if resolved.Event == nil {
				return fmt.Errorf("event at commit %v is nil", resolved.Commit)
			}

			if err := handler(*resolved.Event); err != nil {
				return err
			}
		}

		if subEvent.SubscriptionDropped != nil {
			return nil
		}
	}
}
