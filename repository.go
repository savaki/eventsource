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

type Aggregate interface {
	Apply(event interface{}) bool
}

type Repository struct {
	prototype  reflect.Type
	store      Store
	serializer Serializer
	types      map[string]reflect.Type
	writer     io.Writer
	debug      bool
}

func New(prototype Aggregate, opts ...Option) *Repository {
	t := reflect.TypeOf(prototype)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r := &Repository{
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

func (r *Repository) logf(format string, args ...interface{}) {
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

func (r *Repository) Bind(events ...interface{}) error {
	for _, event := range events {
		if event == nil {
			return errors.New("attempt to bind nil event")
		}

		err := r.serializer.Bind(event)
		if err != nil {
			return err
		}

		meta, err := Inspect(event)
		if err != nil {
			return err
		}

		r.logf("Binding %12s => %#v", meta.EventType, event)

		eventType := reflect.TypeOf(event)
		if eventType.Kind() == reflect.Ptr {
			eventType = eventType.Elem()
		}

		r.types[meta.EventType] = eventType
	}

	return nil
}

func (r *Repository) Save(ctx context.Context, events ...interface{}) error {
	return r.store.Save(ctx, r.serializer, events...)
}

func (r *Repository) Load(ctx context.Context, aggregateID string) (interface{}, error) {
	history, err := r.store.Fetch(ctx, r.serializer, aggregateID, 0)
	if err != nil {
		return nil, err
	}

	if len(history.Events) == 0 {
		return nil, errors.New("not found")
	}

	r.logf("Loaded %v event(s) for aggregate id, %v", len(history.Events), aggregateID)
	v := reflect.New(r.prototype).Interface()

	aggregate := v.(Aggregate) // v is guaranteed to be an Aggregate

	for _, event := range history.Events {
		ok := aggregate.Apply(event)
		if !ok {
			meta, err := Inspect(event)
			if err == nil {
				return nil, errors.New(msgUnhandledEvent + " - " + meta.EventType)
			}
			return nil, errors.New(msgUnhandledEvent)
		}
	}

	return aggregate, nil
}

type Option func(registry *Repository)

func WithStore(store Store) Option {
	return func(registry *Repository) {
		registry.store = store
	}
}

func WithSerializer(serializer Serializer) Option {
	return func(registry *Repository) {
		registry.serializer = serializer
	}
}

func WithDebug(w io.Writer) Option {
	return func(registry *Repository) {
		registry.debug = true
		registry.writer = w
	}
}
