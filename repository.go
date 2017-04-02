package eventsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

const (
	msgUnhandledEvent = "aggregate was unable to handle event"
)

// Aggregate represents the aggregate root in the domain driven design sense.  It aggregates the events and presents the
// current view of the domain object
type Aggregate interface {

	// On will be called for each event processed; returns true if aggregate was able to handle the event, false otherwise
	On(event Event) bool
}

type Repository interface {
	Bind(events ...Event) error
	Load(ctx context.Context, aggregateID string) (Aggregate, error)
	Save(ctx context.Context, events ...Event) error
	New() Aggregate
}

type repository struct {
	prototype  reflect.Type
	store      Store
	serializer Serializer
	types      map[string]reflect.Type
	writer     io.Writer
	debug      bool
}

func New(prototype Aggregate, opts ...Option) Repository {
	t := reflect.TypeOf(prototype)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r := &repository{
		prototype:  t,
		store:      newMemoryStore(),
		serializer: JSONSerializer(),
		types:      map[string]reflect.Type{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *repository) logf(format string, args ...interface{}) {
	if !r.debug {
		return
	}

	now := time.Now().Format(time.StampMilli)
	io.WriteString(r.writer, now)
	io.WriteString(r.writer, " ")

	fmt.Fprintf(r.writer, format, args...)
	if !strings.HasSuffix(format, "\n") {
		io.WriteString(r.writer, "\n")
	}
}

// EventType is a helper func that extracts the event type of the event along with the reflect.Type of the event.
//
// Primarily useful for serializers that need to understand how marshal and unmarshal instances of Event to a []byte
func EventType(event Event) (string, reflect.Type) {
	t := reflect.TypeOf(event)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if v, ok := event.(EventTyper); ok {
		return v.EventType(), t
	}

	return t.Name(), t
}

func (r *repository) Bind(events ...Event) error {
	for _, event := range events {
		if event == nil {
			return errors.New("attempt to bind nil event")
		}

		err := r.serializer.Bind(event)
		if err != nil {
			return err
		}

		eventType, typ := EventType(event)
		r.logf("Binding %12s => %#v", eventType, event)
		r.types[eventType] = typ
	}

	return nil
}

// New returns a new instance of the aggregate
func (r *repository) New() Aggregate {
	return reflect.New(r.prototype).Interface().(Aggregate)
}

func (r *repository) Save(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}

	var aggregateID string
	history := make(History, 0, len(events))
	for _, event := range events {
		record, err := r.serializer.Serialize(event)
		if err != nil {
			return err
		}

		aggregateID = event.AggregateID()

		history = append(history, record)
	}

	return r.store.Save(ctx, aggregateID, history...)
}

func (r *repository) Load(ctx context.Context, aggregateID string) (Aggregate, error) {
	history, err := r.store.Fetch(ctx, aggregateID, 0)
	if err != nil {
		return nil, err
	}

	entryCount := len(history)
	if entryCount == 0 {
		return nil, errors.New("not found")
	}

	r.logf("Loaded %v event(s) for aggregate id, %v", entryCount, aggregateID)
	aggregate := r.New()

	for _, record := range history {
		event, err := r.serializer.Deserialize(record)
		if err != nil {
			return nil, err
		}

		ok := aggregate.On(event)
		if !ok {
			eventType, _ := EventType(event)
			return nil, fmt.Errorf(msgUnhandledEvent + " - " + eventType)
		}
	}

	return aggregate.(Aggregate), nil
}

type Option func(registry *repository)

func WithStore(store Store) Option {
	return func(registry *repository) {
		registry.store = store
	}
}

func WithSerializer(serializer Serializer) Option {
	return func(registry *repository) {
		registry.serializer = serializer
	}
}

func WithDebug(w io.Writer) Option {
	return func(registry *repository) {
		registry.debug = true
		registry.writer = w
	}
}
