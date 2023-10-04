package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

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

	srv := &http.Server{
		Addr:    ":8080",
		Handler: NewReqHandler(ctx, logger, esdbClient, sqlClient),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server's ListenAndServe method returned an error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(timeoutCtx); err != nil {
		slog.Error("server shutdown returned an error", "error", err)
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
	switch req.Method {
	case http.MethodGet:
		h.handleGetAllUsers(res, req)
	case http.MethodPost:
		h.handleCreateUser(res, req)
	}
}

func (h *ReqHandler) handleCreateUser(res http.ResponseWriter, req *http.Request) {
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

func (h *ReqHandler) handleGetAllUsers(res http.ResponseWriter, req *http.Request) {
	users, err := getAllUsers(h.Ctx, h.SqlClient)
	if err != nil {
		h.Log.Error("failed to get all users", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	encoder := json.NewEncoder(res)
	if err := encoder.Encode(users); err != nil {
		h.Log.Error("failed to encode users to json", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
}
