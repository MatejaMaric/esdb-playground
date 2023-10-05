package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
)

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

	exists, err := usernameExists(h.Ctx, h.SqlClient, event.Username)
	if err != nil {
		h.Log.Error("failed to check if the username exists", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	if exists {
		encoder := json.NewEncoder(res)
		if err := encoder.Encode(map[string]string{"error": "username already exists"}); err != nil {
			h.Log.Error("failed to encode 'exists' response to json", "error", err)
			res.WriteHeader(http.StatusInternalServerError)
			return
		}
		return
	}

	eventId, err := uuid.NewV4()
	if err != nil {
		h.Log.Error("failed to create a uuid", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		h.Log.Error("failed to marshal json", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	events := []esdb.EventData{
		{
			EventID:     eventId,
			EventType:   string(CreateUser),
			ContentType: esdb.JsonContentType,
			Data:        data,
		},
	}

	appendResult, err := h.EsdbClient.AppendToStream(h.Ctx, UserStream, esdb.AppendToStreamOptions{}, events...)
	if err != nil {
		h.Log.Error("failed to append to stream", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.Log.Debug("successfully appended to stream", "appendResult", appendResult)

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
