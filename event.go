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

// EventTyper is an optional interface that can be applied to an Event that allows it to specify an event type
// different than the name of the struct
type EventTyper interface {
	// EventType returns the name of event type
	EventType() string
}

// Model provides a default implementation of an Event that is suitable for being embedded
type Model struct {
	// ID contains the AggregateID
	ID string

	// Version holds the event version
	Version int

	// At contains the event time
	At time.Time
}

// AggregateID implements part of the Event interface
func (m Model) AggregateID() string {
	return m.ID
}

// EventVersion implements part of the Event interface
func (m Model) EventVersion() int {
	return m.Version
}

// EventAt implements part of the Event interface
func (m Model) EventAt() time.Time {
	return m.At
}
