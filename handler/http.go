package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
)

type HttpHandler struct {
	Ctx        context.Context
	Log        *slog.Logger
	EsdbClient *esdb.Client
	SqlClient  *sql.DB
}

func NewHttpHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB) *HttpHandler {
	return &HttpHandler{
		Ctx:        ctx,
		Log:        logger,
		EsdbClient: esdbClient,
		SqlClient:  sqlClient,
	}
}

func (h *HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		h.handleGetAllUsers(res, req)
	case http.MethodPost:
		h.handleCreateUser(res, req)
	case http.MethodPatch:
		h.handleUserLogin(res, req)
	}
}

func (h *HttpHandler) handleCreateUser(res http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	var event events.CreateUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		http.Error(res, "failed to decode request", http.StatusBadRequest)
		return
	}

	appendRes, err := AppendCreateUserEvent(h.Ctx, h.EsdbClient, event)
	if err != nil && errors.Is(err, esdb.ErrWrongExpectedStreamRevision) {
		http.Error(res, "user already exists", http.StatusBadRequest)
		return
	}
	if err != nil {
		h.Log.Error("appending to stream resulted in an error", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.Log.Debug("successfully appended to stream",
		"CommitPosition", appendRes.CommitPosition,
		"PreparePosition", appendRes.PreparePosition,
		"NextExpectedVersion", appendRes.NextExpectedVersion,
	)

	res.WriteHeader(http.StatusOK)
}

func (h *HttpHandler) handleGetAllUsers(res http.ResponseWriter, req *http.Request) {
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

func (h *HttpHandler) handleUserLogin(res http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	var event events.LoginUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		http.Error(res, "failed to decode request", http.StatusBadRequest)
		return
	}

	appendRes, err := AppendLoginUserEvent(h.Ctx, h.EsdbClient, event)
	if err != nil && errors.Is(err, esdb.ErrStreamNotFound) {
		http.Error(res, "user does not exists", http.StatusBadRequest)
		return
	}
	if err != nil {
		h.Log.Error("appending to stream resulted in an error", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.Log.Debug("successfully appended to stream",
		"CommitPosition", appendRes.CommitPosition,
		"PreparePosition", appendRes.PreparePosition,
		"NextExpectedVersion", appendRes.NextExpectedVersion,
	)

	res.WriteHeader(http.StatusOK)
}
