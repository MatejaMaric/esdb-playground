package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/projections-playground/events"
	"github.com/go-sql-driver/mysql"
)

func ConnectToEventStoreDB() (*esdb.Client, error) {
	const connectionStr string = "esdb://localhost:2113?tls=false"

	esdbConf, err := esdb.ParseConnectionString(connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EventStoreDB connection string: %w", err)
	}

	return esdb.NewClient(esdbConf)
}

func ConnectToMariaDB() (*sql.DB, error) {
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
		return nil, fmt.Errorf("failed to open the connection to MariaDB: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MariaDB: %w", err)
	}

	return db, nil
}

func InsertUser(ctx context.Context, db *sql.DB, user events.UserStateEvent) (int64, error) {
	result, err := db.ExecContext(ctx, "INSERT INTO users (username, login_count, version) VALUES (?, ?, ?)", user.Username, user.LoginCount, user.Version)
	if err != nil {
		return 0, fmt.Errorf("failed to exec insert command: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed getting the last insert id: %w", err)
	}

	return id, nil
}

func GetUser(ctx context.Context, db *sql.DB, username string) (events.UserStateEvent, error) {
	var user events.UserStateEvent

	query, err := db.PrepareContext(ctx, "SELECT username, login_count, version FROM users WHERE username = ?")
	if err != nil {
		return user, fmt.Errorf("failed to prepare the statement: %w", err)
	}

	row := query.QueryRowContext(ctx, username)
	if err := row.Scan(&user.Username, &user.LoginCount, &user.Version); err != nil {
		return user, fmt.Errorf("failed to get the user %s: %w", username, err)
	}

	return user, nil
}

func GetAllUsers(ctx context.Context, db *sql.DB) ([]events.UserStateEvent, error) {
	var users []events.UserStateEvent

	rows, err := db.QueryContext(ctx, "SELECT username, login_count, version FROM users")
	if err != nil {
		return nil, fmt.Errorf("failed to select users: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var user events.UserStateEvent
		if err := rows.Scan(&user.Username, &user.LoginCount, &user.Version); err != nil {
			return nil, fmt.Errorf("failed to scan the row: %w", err)
		}
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err(): %w", err)
	}

	return users, nil
}

func UpdateUser(ctx context.Context, db *sql.DB, user events.UserStateEvent) (int64, error) {
	stmt, err := db.PrepareContext(ctx, "UPDATE users SET login_count=?, version=? WHERE username=?")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare the statement: %w", err)
	}

	result, err := stmt.ExecContext(ctx, user.LoginCount, user.Version, user.Username)
	if err != nil {
		return 0, fmt.Errorf("failed to exec update command: %w", err)
	}

	num, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get the number of affected rows: %w", err)
	}

	return num, nil
}
