package command

import (
	"context"

	"github.com/savaki/eventsource"
)

type Model struct {
	ID string
}

func (m Model) AggregateID() string {
	return m.ID
}

type Interface interface {
	AggregateID() string
}

type Handler interface {
	Apply(ctx context.Context, command Interface) ([]eventsource.Event, error)
}
