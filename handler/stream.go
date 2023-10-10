package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/projections"
	"github.com/gofrs/uuid"
)

type StreamHandler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	doneCh     chan struct{}
	logger     *slog.Logger
	esdbClient *esdb.Client
	sqlClient  *sql.DB
}

func NewStreamHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) *StreamHandler {
	handlerCtx, cancel := context.WithCancel(ctx)

	return &StreamHandler{
		ctx:        handlerCtx,
		cancel:     cancel,
		doneCh:     make(chan struct{}),
		logger:     logger,
		esdbClient: esdbClient,
		sqlClient:  sqlClient,
	}
}

func (h *StreamHandler) Start() error {
	defer func() {
		h.doneCh <- struct{}{}
	}()
	return handleStream(h.ctx, h.logger, h.esdbClient, h.sqlClient)
}

func (h *StreamHandler) Stop(timeout time.Duration) error {
	h.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return errors.New("failed to finished within timeout")
	case <-h.doneCh:
		return nil
	}
}

func handleStream(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) error {
	stream, err := esdbClient.SubscribeToAll(ctx, esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.StreamFilterType,
			Prefixes: []string{events.UserEventsStream},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %w", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			logger.Error("closing the stream resulted in an error", "error", err)
		} else {
			logger.Debug("stream successfully closed")
		}
	}()

	dbProjection := projections.NewDatabaseProjection(ctx, sqlClient)
	streamProjection := projections.NewStreamProjection(ctx, esdbClient)

	for {
		subEvent := stream.Recv()

		if subEvent.EventAppeared != nil {

			if err := dbProjection.HandleEvent(subEvent.EventAppeared); err != nil {
				logger.Error("database projection event handler returned an error", "error", err)
			} else {
				logger.Debug("database projection handled event",
					"EventNumber", subEvent.EventAppeared.Event.EventNumber,
					"CommitPosition", subEvent.EventAppeared.Event.Position.Commit,
					"PreparePosition", subEvent.EventAppeared.Event.Position.Prepare,
				)
			}

			if err := streamProjection.HandleEvent(subEvent.EventAppeared); err != nil {
				logger.Error("stream projection event handler returned an error", "error", err)
			} else {
				logger.Debug("stream projection handled event",
					"EventNumber", subEvent.EventAppeared.Event.EventNumber,
					"CommitPosition", subEvent.EventAppeared.Event.Position.Commit,
					"PreparePosition", subEvent.EventAppeared.Event.Position.Prepare,
				)
			}

		}

		if subEvent.SubscriptionDropped != nil {
			logger.Info("subscription dropped", "error", subEvent.SubscriptionDropped.Error)
			break
		}
	}

	return nil
}

func AppendCreateUserEvent(ctx context.Context, esdbClient *esdb.Client, event events.CreateUserEvent) (*esdb.WriteResult, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to create a uuid: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}

	eventData := esdb.EventData{
		EventID:     eventId,
		EventType:   string(events.CreateUser),
		ContentType: esdb.JsonContentType,
		Data:        data,
	}

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	streamName := fmt.Sprintf("%s-%s", events.UserEventsStream, event.Username)

	appendResult, err := esdbClient.AppendToStream(ctx, streamName, aopts, eventData)
	if err != nil {
		return nil, fmt.Errorf("failed to append to stream: %w", err)
	}

	return appendResult, nil
}

func AppendLoginUserEvent(ctx context.Context, esdbClient *esdb.Client, event events.LoginUserEvent) (*esdb.WriteResult, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to create a uuid: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}

	eventData := esdb.EventData{
		EventID:     eventId,
		EventType:   string(events.LoginUser),
		ContentType: esdb.JsonContentType,
		Data:        data,
	}

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.StreamExists{},
	}

	streamName := fmt.Sprintf("%s-%s", events.UserEventsStream, event.Username)

	appendResult, err := esdbClient.AppendToStream(ctx, streamName, aopts, eventData)
	if err != nil {
		return nil, fmt.Errorf("failed to append to stream: %w", err)
	}

	return appendResult, nil
}
