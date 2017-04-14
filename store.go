package eventsource

import (
	"context"
	"sort"
	"sync"
)

// History represents the events to be applied to recreate the aggregate in version order
type History []Record

// Record provides the shape of the records to be saved to the db
type Record struct {
	// Version is the event version the Data represents
	Version int

	// At indicates when the event happened; provided as a utility for the store
	At EpochMillis

	// Data contains the Serializer encoded version of the data
	Data []byte
}

// Store provides storage for events
type Store interface {
	// Save saves events to the store
	Save(ctx context.Context, aggregateID string, records ...Record) error

	// Fetch retrieves the History of events with the specified aggregate id
	Fetch(ctx context.Context, aggregateID string, version int) (History, error)
}

// memoryStore provides an in-memory implementation of Store
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
	if aggregateID == "" {
		history := History{}
		for _, events := range m.eventsByID {
			history = append(history, events...)
		}
		return history, nil
	}
	history, ok := m.eventsByID[aggregateID]
	if !ok {
		return nil, NewError(nil, AggregateNotFound, "no aggregate found with id, %v", aggregateID)
	}

	return history, nil
}
