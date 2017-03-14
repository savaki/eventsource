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

type EventHandlerFunc func(ctx context.Context, aggregate, event interface{}) error

func (fn EventHandlerFunc) HandleEvent(ctx context.Context, aggregate, event interface{}) error {
	return fn(ctx, aggregate, event)
}

type EventHandler interface {
	HandleEvent(ctx context.Context, aggregate, event interface{}) error
}

type Registry struct {
	prototype  reflect.Type
	store      Store
	serializer Serializer
	handlers   map[string]EventHandler
	types      map[string]reflect.Type
	writer     io.Writer
	debug      bool
}

func New(prototype interface{}, opts ...Option) *Registry {
	t := reflect.TypeOf(prototype)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r := &Registry{
		prototype:  t,
		store:      newMemoryStore(),
		serializer: JSONSerializer(),
		handlers:   map[string]EventHandler{},
		types:      map[string]reflect.Type{},
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *Registry) logf(format string, args ...interface{}) {
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

func (r *Registry) BindFunc(event interface{}, h EventHandlerFunc) error {
	return r.Bind(event, h)
}

func (r *Registry) Bind(event interface{}, h EventHandler) error {
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

	if _, ok := r.handlers[meta.EventType]; ok {
		return errors.New("handler already defined")
	}

	eventType := reflect.TypeOf(event)
	if eventType.Kind() == reflect.Ptr {
		eventType = eventType.Elem()
	}

	r.types[meta.EventType] = eventType
	r.handlers[meta.EventType] = h
	return nil
}

func (r *Registry) Save(ctx context.Context, events ...interface{}) error {
	return r.store.Save(ctx, r.serializer, events...)
}

func (r *Registry) Fetch(ctx context.Context, aggregateID string, version int) (History, error) {
	return r.store.Fetch(ctx, r.serializer, aggregateID, version)
}

func (r *Registry) Load(ctx context.Context, aggregateID string, version int) (interface{}, int, error) {
	history, err := r.Fetch(ctx, aggregateID, version)
	if err != nil {
		return nil, 0, err
	}

	if len(history.Events) == 0 {
		return nil, version, errors.New("not found")
	}

	r.logf("Loaded %v event(s) for aggregate id, %v", len(history.Events), aggregateID)
	v := reflect.New(r.prototype).Interface()

	highestVersion := 0
	for _, event := range history.Events {
		meta, err := Inspect(event)
		if err != nil {
			return nil, version, err
		}

		h, ok := r.handlers[meta.EventType]
		if !ok {
			return nil, version, errors.New("no handler bound")
		}

		err = h.HandleEvent(ctx, v, event)
		if err != nil {
			return nil, version, err
		}

		highestVersion = meta.Version
	}

	return v, highestVersion, nil
}

type Option func(registry *Registry)

func WithStore(store Store) Option {
	return func(registry *Registry) {
		registry.store = store
	}
}

func WithSerializer(serializer Serializer) Option {
	return func(registry *Registry) {
		registry.serializer = serializer
	}
}

func WithDebug(w io.Writer) Option {
	return func(registry *Registry) {
		registry.debug = true
		registry.writer = w
	}
}
