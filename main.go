package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/handler"
	"github.com/MatejaMaric/esdb-playground/reservation"
	"github.com/MatejaMaric/esdb-playground/utils"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	esdbClient, err := db.ConnectToEventStoreDB()
	if err != nil {
		logger.Error("failed to connect to EventStoreDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to EventStoreDB instance")

	sqlClient, err := db.ConnectToMariaDB()
	if err != nil {
		logger.Error("failed to connect to MariaDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to MariaDB instance")

	redisClient, err := db.ConnectToRedis()
	if err != nil {
		logger.Error("failed to connect to Redis instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to Redis instance")

	srv := &http.Server{
		Addr:    ":8080",
		Handler: handler.NewHttpHandler(ctx, logger, esdbClient, sqlClient, redisClient),
	}

	userReady := make(chan struct{})

	userEventHandler := utils.NewStartStop(ctx, func(stoppableCtx context.Context) error {
		return handler.HandleUserStream(stoppableCtx, logger, esdbClient, sqlClient, userReady)
	})

	reservationHandler := utils.NewStartStop(ctx, func(stoppableCtx context.Context) error {
		return handler.HandleReservationStream(stoppableCtx, logger, esdbClient, redisClient)
	})

	if err := reservation.RepopulateRedis(ctx, esdbClient, redisClient); err != nil {
		logger.Error("failed to repopulate Redis with reservations", "error", err)
		os.Exit(1)
	}

	go func() {
		logger.Debug("starting user event handler")
		if err := userEventHandler.Start(); err != nil {
			logger.Error("event handler returned an error", "error", err)
		}
	}()

	select {
	case <-userReady:
		logger.Debug("user event handler finished processing previous events")
	case <-ctx.Done():
		logger.Debug("interrupt received before user event handler finished processing previous events")
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server's ListenAndServe method returned an error", "error", err)
		}
	}()

	go func() {
		if err := reservationHandler.Start(); err != nil {
			logger.Error("reservation handler returned an error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(timeoutCtx); err != nil {
		logger.Error("server shutdown returned an error", "error", err)
	}

	if err := userEventHandler.Stop(5 * time.Second); err != nil {
		logger.Error("event handler shutdown returned an error", "error", err)
	}

	if err := reservationHandler.Stop(5 * time.Second); err != nil {
		logger.Error("reservation handler shutdown returned an error", "error", err)
	}
}
