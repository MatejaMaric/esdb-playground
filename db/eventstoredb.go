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

type AggregateFunc[T any] func(T, *esdb.RecordedEvent) (T, error)

func AggregateStream[T any](ctx context.Context, esdbClient *esdb.Client, streamName string, fold AggregateFunc[T]) (*T, error) {
	ropts := esdb.ReadStreamOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}

	stream, err := esdbClient.ReadStream(ctx, streamName, ropts, math.MaxUint64)
	if err != nil {
		return nil, fmt.Errorf("failed to read the stream '%s': %w", streamName, err)
	}
	defer stream.Close()

	var user T

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return &user, fmt.Errorf("error while reading events from the stream %s: %w", streamName, err)
		}

		if event.Event == nil {
			return &user, fmt.Errorf("event is nil!")
		}

		user, err = fold(user, event.Event)
		if err != nil {
			return &user, fmt.Errorf("applying the event returned an error: %w", err)
		}
	}

	return &user, nil
}
