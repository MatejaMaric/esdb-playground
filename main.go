package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/EventStore/EventStore-Client-Go/esdb"

	"github.com/go-sql-driver/mysql"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	_, err := connectToEventStoreDB()
	if err != nil {
		logger.Error("failed to connect to EventStoreDB instance", "error", err)
	}
	logger.Info("successfully connected to EventStoreDB instance")

	_, err = connectToMariaDB()
	if err != nil {
		logger.Error("failed to connect to MariaDB instance", "error", err)
	}
	logger.Info("successfully connected to MariaDB instance")
}

func connectToEventStoreDB() (*esdb.Client, error) {
	const connectionStr string = "esdb://localhost:2113?tls=false"

	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EventStoreDB connection string: %v", err)
	}

	return esdb.NewClient(esdbConf)
}

func connectToMariaDB() (*sql.DB, error) {
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
		return nil, fmt.Errorf("failed to open the connection to mariadb: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping mariadb: %v", err)
	}

	return db, nil
}
