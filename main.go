package main

import (
	"database/sql"
	"log/slog"
	"os"

	"github.com/EventStore/EventStore-Client-Go/esdb"

	"github.com/go-sql-driver/mysql"
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

	cfg := mysql.Config{
		Net:                  "tcp",
		Addr:                 "127.0.0.1:3306",
		DBName:               "projected_models",
		User:                 "playground_user",
		Passwd:               "playground_user_password",
		AllowNativePasswords: true,
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		logger.Error("failed to open the connection to mariadb", "error", err)
		os.Exit(1)
	}

	if err := db.Ping(); err != nil {
		logger.Error("failed to ping mariadb", "error", err)
		os.Exit(1)
	}
	logger.Debug("successfully connected to MariaDB instance")
}
