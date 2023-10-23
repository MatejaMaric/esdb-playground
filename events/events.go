package events

import "fmt"

type Stream string

const (
	UserEventsStream Stream = "user_events"
	UserStateStream  Stream = "user_state"
)

func (s Stream) ForUser(username string) string {
	return fmt.Sprintf("%s-%s", s, username)
}

type Event string

const (
	UserState  Event = "UserState"
	CreateUser Event = "CreateUser"
	LoginUser  Event = "LoginUser"
)

type UserStateEvent struct {
	Username   string `json:"username"`
	Email      string `json:"email"`
	LoginCount int32  `json:"login_count"`
	Version    uint64 `json:"version"`
}

type CreateUserEvent struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

type LoginUserEvent struct {
	Username string `json:"username"`
}
