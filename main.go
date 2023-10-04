package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

type User struct {
	Id         int64
	Username   string
	LoginCount int32
}

type CreateUserEvent struct {
	Username string
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	esdbClient, err := connectToEventStoreDB()
	if err != nil {
		logger.Error("failed to connect to EventStoreDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to EventStoreDB instance")

	sqlClient, err := connectToMariaDB()
	if err != nil {
		logger.Error("failed to connect to MariaDB instance", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to MariaDB instance")

	id, err := insertUser(ctx, sqlClient, User{Username: "testuser"})
	logger.Debug("insertUser results", "id", id, "error", err)

	exists, err := usernameExists(ctx, sqlClient, "testuser")
	logger.Debug("usernameExists results", "exists", exists, "error", err)

	exists, err = usernameExists(ctx, sqlClient, "another_user")
	logger.Debug("usernameExists results", "exists", exists, "error", err)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: NewReqHandler(ctx, logger, esdbClient, sqlClient),
	}

	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server's ListenAndServe method returned an error", "error", err)
	}
}

type ReqHandler struct {
	Ctx        context.Context
	Log        *slog.Logger
	EsdbClient *esdb.Client
	SqlClient  *sql.DB
}

func NewReqHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) *ReqHandler {
	return &ReqHandler{
		Ctx:        ctx,
		Log:        logger,
		EsdbClient: esdbClient,
		SqlClient:  sqlClient,
	}
}

func (h *ReqHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	var event CreateUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		h.Log.Error("failed to decode request", "error", err)
		res.WriteHeader(http.StatusBadRequest)
		return
	}

	res.WriteHeader(http.StatusOK)
}
