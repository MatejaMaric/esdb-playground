package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/MatejaMaric/esdb-playground/projections-playground/db"
	"github.com/MatejaMaric/esdb-playground/projections-playground/handler"
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

	srv := &http.Server{
		Addr:    ":8080",
		Handler: handler.NewHttpHandler(ctx, logger, esdbClient, sqlClient),
	}

	eventHandler := handler.NewStreamHandler(ctx, logger, esdbClient, sqlClient)

	go func() {
		if err := eventHandler.Start(); err != nil {
			logger.Error("event handler returned an error", "error", err)
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server's ListenAndServe method returned an error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(timeoutCtx); err != nil {
		logger.Error("server shutdown returned an error", "error", err)
	}

	if err := eventHandler.Stop(5 * time.Second); err != nil {
		logger.Error("event handler shutdown returned an error", "error", err)
	}
}
