package eventsourceiface

import (
	"context"

	"github.com/savaki/eventsource"
)

type RepositoryAPI interface {
	Bind(events ...eventsource.Event) error
	Load(ctx context.Context, aggregateID string) (eventsource.Aggregate, error)
	Save(ctx context.Context, events ...eventsource.Event) error
	New() eventsource.Aggregate
}
