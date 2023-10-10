package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/gofrs/uuid"
)

type Event string

const (
	CreateUser Event = "CreateUser"
)

type CreateUserEvent struct {
	Username string `json:"username"`
}

const UserStream string = "user_events"

type Handler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	doneCh     chan struct{}
	logger     *slog.Logger
	esdbClient *esdb.Client
	sqlClient  *sql.DB
}

func NewHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) *Handler {
	handlerCtx, cancel := context.WithCancel(ctx)

	return &Handler{
		ctx:        handlerCtx,
		cancel:     cancel,
		doneCh:     make(chan struct{}),
		logger:     logger,
		esdbClient: esdbClient,
		sqlClient:  sqlClient,
	}
}

func (h *Handler) Start() error {
	defer func() {
		h.doneCh <- struct{}{}
	}()
	return handleStream(h.ctx, h.logger, h.esdbClient, h.sqlClient)
}

func (h *Handler) Stop(timeout time.Duration) error {
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
			Prefixes: []string{UserStream},
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

	for {
		subEvent := stream.Recv()

		if subEvent.EventAppeared != nil {
			if err := handleEvent(ctx, sqlClient, subEvent.EventAppeared); err != nil {
				logger.Error("event handler returned an error", "error", err)
			} else {
				logger.Debug("handled event",
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

func handleEvent(ctx context.Context, sqlClient *sql.DB, resolved *esdb.ResolvedEvent) error {
	if resolved.Event == nil {
		return errors.New("resolved.Event is nil")
	}

	switch resolved.Event.EventType {
	case string(CreateUser):
		return handleCreateUserEvent(ctx, sqlClient, resolved.Event)
	default:
		return fmt.Errorf("unknown event type: %s", resolved.Event.EventType)
	}
}

func handleCreateUserEvent(ctx context.Context, sqlClient *sql.DB, rawEvent *esdb.RecordedEvent) error {
	var event CreateUserEvent
	if err := json.Unmarshal(rawEvent.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	user := db.User{
		Username:   event.Username,
		LoginCount: 0,
		Version:    rawEvent.EventNumber,
	}

	if _, err := db.InsertUser(ctx, sqlClient, user); err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

func AppendCreateUserEvent(ctx context.Context, esdbClient *esdb.Client, event CreateUserEvent) (*esdb.WriteResult, error) {
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
		EventType:   string(CreateUser),
		ContentType: esdb.JsonContentType,
		Data:        data,
	}

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	streamName := fmt.Sprintf("%s-%s", UserStream, event.Username)

	appendResult, err := esdbClient.AppendToStream(ctx, streamName, aopts, eventData)
	if err != nil {
		return nil, fmt.Errorf("failed to append to stream: %w", err)
	}

	return appendResult, nil
}
