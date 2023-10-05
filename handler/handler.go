package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/gofrs/uuid"
)

type Handler struct {
	Ctx        context.Context
	Log        *slog.Logger
	EsdbClient *esdb.Client
	SqlClient  *sql.DB
}

func New(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) *Handler {
	return &Handler{
		Ctx:        ctx,
		Log:        logger,
		EsdbClient: esdbClient,
		SqlClient:  sqlClient,
	}
}

func (h *Handler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		h.handleGetAllUsers(res, req)
	case http.MethodPost:
		h.handleCreateUser(res, req)
	}
}

func (h *Handler) handleCreateUser(res http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	var event events.CreateUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		h.Log.Error("failed to decode request", "error", err)
		res.WriteHeader(http.StatusBadRequest)
		return
	}

	exists, err := db.UsernameExists(h.Ctx, h.SqlClient, event.Username)
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

	eventData := esdb.EventData{
		EventID:     eventId,
		EventType:   string(events.CreateUser),
		ContentType: esdb.JsonContentType,
		Data:        data,
	}

	appendResult, err := h.EsdbClient.AppendToStream(h.Ctx, events.UserStream, esdb.AppendToStreamOptions{}, eventData)
	if err != nil {
		h.Log.Error("failed to append to stream", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.Log.Debug("successfully appended to stream", "appendResult", appendResult)

	res.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetAllUsers(res http.ResponseWriter, req *http.Request) {
	users, err := db.GetAllUsers(h.Ctx, h.SqlClient)
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
