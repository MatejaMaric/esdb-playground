package events

const (
	UserEventsStream string = "user_events"
	UserStateStream  string = "user_state"
)

type Event string

const (
	UserState  Event = "UserState"
	CreateUser Event = "CreateUser"
	LoginUser  Event = "LoginUser"
)

type UserStateEvent struct {
	Username   string `json:"username"`
	LoginCount int32  `json:"login_count"`
	Version    uint64 `json:"version"`
}

type CreateUserEvent struct {
	Username string `json:"username"`
}

type LoginUserEvent struct {
	Username string `json:"username"`
}
