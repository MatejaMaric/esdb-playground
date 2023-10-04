package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"golang.org/x/net/context"

	"github.com/go-sql-driver/mysql"
)

type User struct {
	Id         int64
	Username   string
	LoginCount int32
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	_, err := connectToEventStoreDB()
	if err != nil {
		logger.Error("failed to connect to EventStoreDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to EventStoreDB instance")

	db, err := connectToMariaDB()
	if err != nil {
		logger.Error("failed to connect to MariaDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to MariaDB instance")

	id, err := insertUser(ctx, db, User{Username: "testuser"})
	if err != nil {
		logger.Error("failed inseting user into MariaDB", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully inserted user into MariaDB", "id", id)
}

func insertUser(ctx context.Context, db *sql.DB, user User) (int64, error) {
	result, err := db.ExecContext(ctx, "INSERT INTO users (username, login_count) VALUES (?, ?)", user.Username, user.LoginCount)
	if err != nil {
		return 0, fmt.Errorf("failed to exec insert command: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed getting the last insert id: %v", err)
	}

	return id, nil
}

func getAllUser(ctx context.Context, db *sql.DB) ([]User, error) {
	var users []User

	rows, err := db.QueryContext(ctx, "SELECT id, username, login_count FROM users")
	if err != nil {
		return nil, fmt.Errorf("failed to select users: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var user User
		if err := rows.Scan(&user.Id, &user.Username, &user.LoginCount); err != nil {
			return nil, fmt.Errorf("failed to scan the row: %v", err)
		}
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err(): %v", err)
	}

	return users, nil
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
