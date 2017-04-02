package command

import (
	"context"
	"reflect"

	"github.com/savaki/eventsource"
)

const (
	CodePreprocessorErr            = "PreprocessorErr"
	CodeEventLoadErr               = "EventLoadErr"
	CodeAggregateNotCommandHandler = "AggregateNotCommandHandler"
	CodeHandlerErr                 = "HandlerErr"
	CodeSaveErr                    = "SaveErr"
)

// Constructor is an interface that a Command may implement to indicate the Command is the "constructor"
type Constructor interface {
	New() bool
}

// Preprocessor manipulates commands prior to them being executed by the Handler
type Preprocessor interface {
	// Before is executed prior to the Handler.Apply call
	Before(ctx context.Context, command Interface) error
}

// Dispatcher manages the execution of a command
type Dispatcher interface {
	// Dispatch retrieves the Aggregate from the Repository, applies the Handler, and saves the result to the Repository
	Dispatch(ctx context.Context, command Interface) error
}

type dispatchFunc func(ctx context.Context, command Interface) error

func (fn dispatchFunc) Dispatch(ctx context.Context, command Interface) error {
	return fn(ctx, command)
}

// New instantiates a new Dispatcher using the Repository and optional Preprocessors provided
func New(repo eventsource.Repository, preprocessors ...Preprocessor) Dispatcher {
	return dispatchFunc(func(ctx context.Context, cmd Interface) error {
		for _, p := range preprocessors {
			err := p.Before(ctx, cmd)
			if err != nil {
				return eventsource.NewError(err, CodePreprocessorErr, "processor failed on command, %#v", cmd)
			}
		}

		var aggregate eventsource.Aggregate
		if v, ok := cmd.(Constructor); ok && v.New() {
			aggregate = repo.New()

		} else {
			aggregateID := cmd.AggregateID()
			v, err := repo.Load(ctx, aggregateID)
			if err != nil {
				return eventsource.NewError(err, CodeEventLoadErr, "Unable to load %v [%v]", typeOf(repo.New()), aggregateID)
			}
			aggregate = v
		}

		handler, ok := aggregate.(Handler)
		if !ok {
			return eventsource.NewError(nil, CodeAggregateNotCommandHandler, "%#v does not implement command.Handler", typeOf(aggregate))
		}

		events, err := handler.Apply(ctx, cmd)
		if err != nil {
			return eventsource.NewError(err, CodeHandlerErr, "Failed to apply command, %v, to aggregate, %v", typeOf(cmd), typeOf(aggregate))
		}

		err = repo.Save(ctx, events...)
		if err != nil {
			return eventsource.NewError(err, CodeSaveErr, "Failed to save events for %v, %v", typeOf(aggregate), cmd.AggregateID())
		}

		return nil
	})
}

func typeOf(aggregate interface{}) string {
	t := reflect.TypeOf(aggregate)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}
