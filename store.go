package eventsource

import (
	"context"
	"sync"
)

type History struct {
	Version int
	Events  []interface{}
}

type Store interface {
	Save(ctx context.Context, serializer Serializer, events ...interface{}) error
	Fetch(ctx context.Context, serializer Serializer, aggregateID string, version int) (History, error)
}

type memoryStore struct {
	mux        *sync.Mutex
	aggregates map[string][]EventMeta
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		mux:        &sync.Mutex{},
		aggregates: map[string][]EventMeta{},
	}
}

func (m *memoryStore) Save(ctx context.Context, serializer Serializer, events ...interface{}) error {
	for _, event := range events {
		meta, err := Inspect(event)
		if err != nil {
			return err
		}

		v, ok := m.aggregates[meta.ID]
		if !ok {
			v = make([]EventMeta, 0, len(events))
			m.aggregates[meta.ID] = v
		}

		m.aggregates[meta.ID] = append(v, meta)
	}

	return nil
}

func (m *memoryStore) Fetch(ctx context.Context, serializer Serializer, aggregateID string, version int) (History, error) {
	v, ok := m.aggregates[aggregateID]
	if !ok {
		return History{}, ErrNotFound
	}

	highestVersion := 0

	events := make([]interface{}, 0, len(v))
	for _, meta := range v {
		if version > 0 && meta.Version > version {
			break
		}
		events = append(events, meta.Event)
		highestVersion = meta.Version
	}

	return History{
		Version: highestVersion,
		Events:  events,
	}, nil
}
