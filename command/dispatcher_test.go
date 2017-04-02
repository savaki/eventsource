package command_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/command"
	"github.com/stretchr/testify/assert"
)

type User struct {
	eventsource.Model
	Name  string
	Email string
}

// -- Events --------------------------------------------

type UserCreated struct {
	eventsource.Model
	Name  string
	Email string
}

type UserEmailChanged struct {
	eventsource.Model
	Email string
}

func (u *User) On(event eventsource.Event) bool {
	switch v := event.(type) {
	case *UserCreated:
		u.Name = v.Name
		u.Email = v.Email

	case *UserEmailChanged:
		u.Email = v.Email

	default:
		return false
	}

	u.ID = event.AggregateID()
	u.Version = event.EventVersion()
	u.At = event.EventAt()

	return true
}

// -- Commands ------------------------------------------

type CreateCommand struct {
	command.Model
	Name  string
	Email string
}

func (c CreateCommand) New() bool {
	return true
}

type ChangeEmailCommand struct {
	command.Model
	Email string
}

func (u *User) Apply(ctx context.Context, cmd command.Interface) ([]eventsource.Event, error) {
	switch v := cmd.(type) {
	case CreateCommand:
		return []eventsource.Event{
			UserCreated{
				Model: eventsource.Model{ID: v.ID, Version: u.Version + 1, At: time.Now()},
				Name:  v.Name,
				Email: v.Email,
			},
		}, nil

	case ChangeEmailCommand:
		return []eventsource.Event{
			UserEmailChanged{
				Model: eventsource.Model{ID: v.ID, Version: u.Version + 1, At: time.Now()},
				Email: v.Email,
			},
		}, nil

	default:
		return nil, fmt.Errorf("command not found, %#v", cmd)
	}
}

func TestLifecycle(t *testing.T) {
	repo := eventsource.New(&User{})
	repo.Bind(UserCreated{}, UserEmailChanged{})

	ctx := context.Background()
	id := "123"
	name := "John Doe"
	email := "joe.doe@example.com"
	updatedEmail := "jane.doe@example.com"

	dispatcher := command.New(repo)
	err := dispatcher.Dispatch(ctx, CreateCommand{
		Model: command.Model{ID: id},
		Name:  name,
		Email: email,
	})
	assert.Nil(t, err)

	err = dispatcher.Dispatch(ctx, ChangeEmailCommand{
		Model: command.Model{ID: id},
		Email: updatedEmail,
	})
	assert.Nil(t, err)

	v, err := repo.Load(ctx, id)
	assert.Nil(t, err)

	user, ok := v.(*User)
	assert.True(t, ok)
	assert.Equal(t, updatedEmail, user.Email)
	assert.Equal(t, name, user.Name)
}
