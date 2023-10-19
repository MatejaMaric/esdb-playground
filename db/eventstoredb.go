package db

import (
	"context"
	"encoding/json"
	"fmt"

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
