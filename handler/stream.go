package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/projections"
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
			Prefixes: []string{string(events.UserEventsStream)},
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
	return db.AppendEvent(
		ctx,
		esdbClient,
		events.UserEventsStream.ForUser(event.Username),
		string(events.CreateUser),
		event,
		esdb.NoStream{},
	)
}

func AppendLoginUserEvent(ctx context.Context, esdbClient *esdb.Client, event events.LoginUserEvent) (*esdb.WriteResult, error) {
	return db.AppendEvent(
		ctx,
		esdbClient,
		events.UserEventsStream.ForUser(event.Username),
		string(events.LoginUser),
		event,
		esdb.StreamExists{},
	)
}