package eventsource

import "time"

// Event describes a change that happened to the aggregate.
//
// * Past tense, EmailChanged
// * Contains intent, EmailChanged is better than EmailSet
type Event interface {
	// AggregateID returns the aggregate id of the event
	AggregateID() string

	// Version contains version number of aggregate
	EventVersion() int

	// At indicates when the event took place
	EventAt() time.Time
}

// EventTyper is an optional interface that allows an event to specify a name different than the name of the struct
type EventTyper interface {
	// EventType returns the type of event
	EventType() string
}

type Model struct {
	ID      string
	Version int
	At      time.Time
}

func (m Model) AggregateID() string {
	return m.ID
}

func (m Model) EventVersion() int {
	return m.Version
}

func (m Model) EventAt() time.Time {
	return m.At
}
