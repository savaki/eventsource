package eventsource

import (
	"context"
	"errors"
	"reflect"
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

	if _, ok := r.handlers[meta.AggregateType]; ok {
		return errors.New("handler already defined")
	}

	eventType := reflect.TypeOf(event)
	if eventType.Kind() == reflect.Ptr {
		eventType = eventType.Elem()
	}

	r.types[meta.AggregateType] = eventType
	r.handlers[meta.AggregateType] = h
	return nil
}

func (r *Registry) Save(ctx context.Context, events ...interface{}) error {
	return r.store.Save(ctx, r.serializer, events...)
}

func (r *Registry) Fetch(ctx context.Context, aggregateID string, version int) ([]interface{}, error) {
	return r.store.Fetch(ctx, r.serializer, aggregateID, version)
}

func (r *Registry) Load(ctx context.Context, aggregateID string) (interface{}, error) {
	return r.LoadVersion(ctx, aggregateID, 0)
}

func (r *Registry) LoadVersion(ctx context.Context, aggregateID string, version int) (interface{}, error) {
	events, err := r.Fetch(ctx, aggregateID, version)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, errors.New("not found")
	}

	v := reflect.New(r.prototype).Interface()
	err = setAggregateID(v, aggregateID)
	if err != nil {
		return nil, err
	}

	for _, event := range events {
		meta, err := Inspect(event)
		if err != nil {
			return nil, err
		}

		h, ok := r.handlers[meta.AggregateType]
		if !ok {
			return nil, errors.New("no handler bound")
		}

		err = h.HandleEvent(ctx, v, event)
		if err != nil {
			return nil, err
		}
	}

	return v, nil
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
