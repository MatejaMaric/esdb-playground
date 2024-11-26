package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/MatejaMaric/esdb-playground/db"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/reservation"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/redis/go-redis/v9"
)

type HttpHandlerContext struct {
	Ctx         context.Context
	Log         *slog.Logger
	EsdbClient  *esdb.Client
	SqlClient   *sql.DB
	RedisClient *redis.Client
}

type CustomHttpHandler[T any] func(*HttpHandlerContext, *http.Request) (int, T, error)

func WrapHandler[T any](h *HttpHandlerContext, handler CustomHttpHandler[T]) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		status, res, err := handler(h, r)

		var dataToBeMarshaled any
		if err != nil {
			dataToBeMarshaled = map[string]string{
				"error": err.Error(),
			}
		} else {
			dataToBeMarshaled = res
		}

		data, err := json.Marshal(dataToBeMarshaled)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if status == http.StatusNoContent {
			w.WriteHeader(status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(data)
	}
}

func NewHttpHandler(ctx context.Context, logger *slog.Logger, esdbClient *esdb.Client, sqlClient *sql.DB, redisClient *redis.Client) http.Handler {
	hndCtx := &HttpHandlerContext{
		Ctx:         ctx,
		Log:         logger,
		EsdbClient:  esdbClient,
		SqlClient:   sqlClient,
		RedisClient: redisClient,
	}

	router := http.NewServeMux()
	router.HandleFunc("GET /", WrapHandler(hndCtx, handleGetUsers))
	router.HandleFunc("POST /", WrapHandler(hndCtx, handleCreateUser))
	router.HandleFunc("PATCH /", WrapHandler(hndCtx, handleUserLogin))

	return router
}

func handleCreateUser(h *HttpHandlerContext, req *http.Request) (int, any, error) {
	defer req.Body.Close()

	var event events.CreateUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		return http.StatusBadRequest, nil, fmt.Errorf("failed to decode request: %w", err)
	}

	emailReservation, err := reservation.CreateReservation(h.Ctx, h.RedisClient, event.Email)
	if err != nil {
		return http.StatusBadRequest, nil, fmt.Errorf("email already registered: %w", err)
	}

	if wr, err := reservation.SaveReservation(h.Ctx, h.EsdbClient, emailReservation); err != nil {
		return http.StatusInternalServerError, nil, fmt.Errorf("appending a reservation event to stream resulted in an error: %w", err)
	} else {
		h.Log.Info("SaveReservation succeeded",
			"emailReservation", emailReservation,
			"WriteResult", wr,
		)
	}

	appendRes, err := db.AppendCreateUserEvent(h.Ctx, h.EsdbClient, event)
	if esdbErr, isNil := esdb.FromError(err); !isNil && esdbErr.Code() == esdb.ErrorCodeWrongExpectedVersion {
		return http.StatusBadRequest, nil, errors.New("user already exists")
	}
	if err != nil {
		return http.StatusInternalServerError, nil, fmt.Errorf("appending to stream resulted in an error: %w", err)
	}
	h.Log.Debug("successfully appended to stream",
		"CommitPosition", appendRes.CommitPosition,
		"PreparePosition", appendRes.PreparePosition,
		"NextExpectedVersion", appendRes.NextExpectedVersion,
	)

	return http.StatusOK, nil, nil
}

func handleGetUsers(h *HttpHandlerContext, req *http.Request) (int, any, error) {
	query := req.URL.Query()
	if query.Has("username") {
		user, err := db.NewUserFromStream(h.Ctx, h.EsdbClient, query.Get("username"))
		if err != nil {
			return http.StatusInternalServerError, nil, fmt.Errorf("failed to aggregate user data: %w", err)
		}

		return http.StatusOK, user, nil
	}

	users, err := db.GetAllUsers(h.Ctx, h.SqlClient)
	if err != nil {
		return http.StatusInternalServerError, nil, fmt.Errorf("failed to get all users: %w", err)
	}

	return http.StatusOK, users, nil
}

func handleUserLogin(h *HttpHandlerContext, req *http.Request) (int, any, error) {
	defer req.Body.Close()

	var event events.LoginUserEvent
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&event); err != nil {
		return http.StatusBadRequest, nil, fmt.Errorf("failed to decode request: %w", err)
	}

	appendRes, err := db.AppendLoginUserEvent(h.Ctx, h.EsdbClient, event)
	if esdbErr, isNil := esdb.FromError(err); !isNil && esdbErr.Code() == esdb.ErrorCodeResourceNotFound {
		return http.StatusBadRequest, nil, fmt.Errorf("user does not exists")
	}
	if err != nil {
		return http.StatusInternalServerError, nil, fmt.Errorf("appending to stream resulted in an error: %w", err)
	}
	h.Log.Debug("successfully appended to stream",
		"CommitPosition", appendRes.CommitPosition,
		"PreparePosition", appendRes.PreparePosition,
		"NextExpectedVersion", appendRes.NextExpectedVersion,
	)

	return http.StatusOK, nil, nil
}
