package eventsource

import (
	"context"
	"sort"
	"sync"
)

type History []Record

type Record struct {
	Version int
	At      EpochMillis
	Data    []byte
}

type Store interface {
	Save(ctx context.Context, aggregateID string, records ...Record) error
	Fetch(ctx context.Context, aggregateID string, version int) (History, error)
}

type memoryStore struct {
	mux        *sync.Mutex
	eventsByID map[string]History
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		mux:        &sync.Mutex{},
		eventsByID: map[string]History{},
	}
}

func (m *memoryStore) Save(ctx context.Context, aggregateID string, records ...Record) error {
	if _, ok := m.eventsByID[aggregateID]; !ok {
		m.eventsByID[aggregateID] = History{}
	}

	history := append(m.eventsByID[aggregateID], records...)
	sort.Slice(history, func(i, j int) bool { return history[i].Version < history[j].Version })
	m.eventsByID[aggregateID] = history

	return nil
}

func (m *memoryStore) Fetch(ctx context.Context, aggregateID string, version int) (History, error) {
	history, ok := m.eventsByID[aggregateID]
	if !ok {
		return nil, NewError(nil, AggregateNotFound, "no aggregate found with id, %v", aggregateID)
	}

	return history, nil
}
