package command

import (
	"context"

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
	return dispatchFunc(func(ctx context.Context, command Interface) error {
		for _, p := range preprocessors {
			err := p.Before(ctx, command)
			if err != nil {
				return eventsource.NewError(err, CodePreprocessorErr, "processor failed on command, %#v", command)
			}
		}

		var aggregate eventsource.Aggregate
		if v, ok := command.(Constructor); ok && v.New() {
			aggregate = repo.New()

		} else {
			aggregateID := command.AggregateID()
			v, err := repo.Load(ctx, aggregateID)
			if err != nil {
				return eventsource.NewError(err, CodeEventLoadErr, "unable to load %#v, %#v", repo.New(), aggregateID)
			}
			aggregate = v
		}

		handler, ok := aggregate.(Handler)
		if !ok {
			return eventsource.NewError(nil, CodeAggregateNotCommandHandler, "%#v does not implement command.Handler", aggregate)
		}

		events, err := handler.Apply(ctx, command)
		if err != nil {
			return eventsource.NewError(err, CodeHandlerErr, "%#v does not implement command.Handler", aggregate)
		}

		err = repo.Save(ctx, events...)
		if err != nil {
			return eventsource.NewError(err, CodeSaveErr, "failed to save events for %#v, %#v", aggregate, command.AggregateID())
		}

		return nil
	})
}
