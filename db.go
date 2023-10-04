package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/go-sql-driver/mysql"
)

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
		return nil, fmt.Errorf("failed to open the connection to MariaDB: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MariaDB: %v", err)
	}

	return db, nil
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

func usernameExists(ctx context.Context, db *sql.DB, username string) (bool, error) {
	query, err := db.PrepareContext(ctx, "SELECT id FROM users WHERE username = ?")
	if err != nil {
		return true, fmt.Errorf("failed to prepare the statement: %v", err)
	}

	rows, err := query.QueryContext(ctx, username)
	if err != nil {
		return true, fmt.Errorf("failed to query: %v", err)
	}

	exists := rows.Next()
	if err := rows.Err(); err != nil {
		return true, fmt.Errorf("rows.Next() produced an error: %v", err)
	}

	return exists, nil
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
