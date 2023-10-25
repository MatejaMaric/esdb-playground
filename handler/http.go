package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/MatejaMaric/esdb-playground/aggregates"
	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/reservation"
	"github.com/redis/go-redis/v9"
)

type HttpHandler struct {
	Ctx         context.Context
	Log         *slog.Logger
	EsdbClient  *esdb.Client
	SqlClient   *sql.DB
	RedisClient *redis.Client
}

func NewHttpHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB, redisClient *redis.Client) *HttpHandler {
	return &HttpHandler{
		Ctx:         ctx,
		Log:         logger,
		EsdbClient:  esdbClient,
		SqlClient:   sqlClient,
		RedisClient: redisClient,
	}
}

func (h *HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		h.handleGetUsers(res, req)
	case http.MethodPost:
		h.handleCreateUser(res, req)
	case http.MethodPatch:
		h.handleUserLogin(res, req)
	}
}

func (h *HttpHandler) writeJson(res http.ResponseWriter, req *http.Request, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		h.Log.Error("json encoding failed", "error", err)
		http.Error(res, "json encoding failed", http.StatusInternalServerError)
	}

	if _, err := res.Write(jsonData); err != nil {
		h.Log.Warn("writing to http response writer returned an error", "error", err)
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

	emailReservation, err := reservation.CreateReservation(h.Ctx, h.RedisClient, event.Email)
	if err != nil {
		http.Error(res, "email already registered", http.StatusBadRequest)
		return
	}

	if _, err := reservation.SaveReservation(h.Ctx, h.EsdbClient, emailReservation); err != nil {
		h.Log.Error("appending a reservation event to stream resulted in an error", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
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

func (h *HttpHandler) handleGetUsers(res http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	if query.Has("username") {
		user, err := aggregates.NewUserAggregate(h.Ctx, h.EsdbClient, query.Get("username"))
		if err != nil {
			h.Log.Error("failed to aggregate user data", "error", err)
			res.WriteHeader(http.StatusInternalServerError)
			return
		}

		h.writeJson(res, req, user)
		return
	}

	users, err := db.GetAllUsers(h.Ctx, h.SqlClient)
	if err != nil {
		h.Log.Error("failed to get all users", "error", err)
		res.WriteHeader(http.StatusInternalServerError)
		return
	}

	h.writeJson(res, req, users)
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
