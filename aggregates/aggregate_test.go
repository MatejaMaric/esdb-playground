package aggregates_test

import (
	"strings"
	"testing"

	"github.com/MatejaMaric/esdb-playground/aggregates"
	"github.com/MatejaMaric/esdb-playground/events"
	"github.com/MatejaMaric/esdb-playground/utils"
	"github.com/go-test/deep"
)

func TestUserApply(t *testing.T) {
	reArr := utils.FakeRecordedEvents(events.UserEventsStream.ForUser("test"), []utils.FakeEvent{
		{events.CreateUser, events.CreateUserEvent{"test", "test@test.com"}},
		{events.LoginUser, events.LoginUserEvent{"test"}},
		{events.LoginUser, events.LoginUserEvent{"test"}},
	})

	ua := aggregates.User{}
	var err error

	for _, re := range reArr {
		ua, err = ua.Apply(re)
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedUa := aggregates.User{"test", "test@test.com", 2, 2}

	if diff := deep.Equal(expectedUa, ua); diff != nil {
		t.Fatalf("unexpected user aggregate:\n%v\n", strings.Join(diff, "\n"))
	}
}
