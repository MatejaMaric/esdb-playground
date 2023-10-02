package main

import (
	"log/slog"
	"os"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	const connectionStr string = "esdb://localhost:2113?tls=false"

	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		logger.Error("failed to parse EventStoreDB connection string",
			"connection_string", connectionStr,
			"error", err,
		)
		os.Exit(1)
	}
	logger.Info("EventStoreDB connection string successfully parsed",
		"configuration_object", esdbConf,
	)

	esdb.NewGrpcClient(*esdbConf)
}
