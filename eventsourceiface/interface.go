package eventsourceiface

import (
	"context"

	"github.com/savaki/eventsource"
)

type RepositoryAPI interface {
	Bind(events ...interface{}) error
	Load(ctx context.Context, aggregateID string) (interface{}, error)
	Save(ctx context.Context, events ...interface{}) error
	New() eventsource.Aggregate
}
