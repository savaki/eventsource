package eventsource_test

import (
	"context"
	"testing"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type Org struct {
	ID   string `eventsource:"id"`
	Name string
}

type OrgNameSet struct {
	eventsource.Model
	Name string
}

func SetOrgName(ctx context.Context, aggregate, event interface{}) error {
	e := event.(OrgNameSet)
	org := aggregate.(*Org)
	org.Name = e.Name
	return nil
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()
	aggregateID := "123"
	name := "Jones"

	t.Run("simple", func(t *testing.T) {
		registry := eventsource.New(Org{})
		registry.BindFunc(OrgNameSet{}, SetOrgName)

		// Test - Add an event to the store and verify we can recreate the object

		err := registry.Save(ctx, OrgNameSet{
			Model: eventsource.Model{ID: aggregateID, Version: 0},
			Name:  name,
		})
		assert.Nil(t, err)

		v, n, err := registry.Load(ctx, aggregateID)
		assert.Nil(t, err)
		assert.Equal(t, 0, n)

		org, ok := v.(*Org)
		assert.True(t, ok)
		assert.Equal(t, aggregateID, org.ID)
		assert.Equal(t, name, org.Name)

		// Test - Update the org name and verify that the change is reflected in the loaded result

		updated := "Sarah"
		err = registry.Save(ctx, OrgNameSet{
			Model: eventsource.Model{ID: aggregateID, Version: 0},
			Name:  updated,
		})
		assert.Nil(t, err)

		v, n, err = registry.Load(ctx, aggregateID)
		assert.Nil(t, err)
		assert.Equal(t, 0, n)

		org, ok = v.(*Org)
		assert.True(t, ok)
		assert.Equal(t, aggregateID, org.ID)
		assert.Equal(t, updated, org.Name)
	})

	t.Run("with pointer prototype", func(t *testing.T) {
		registry := eventsource.New(&Org{})
		registry.BindFunc(OrgNameSet{}, SetOrgName)

		err := registry.Save(ctx, OrgNameSet{
			Model: eventsource.Model{ID: aggregateID, Version: 0},
			Name:  name,
		})
		assert.Nil(t, err)

		v, n, err := registry.Load(ctx, aggregateID)
		assert.Nil(t, err)
		assert.Equal(t, 0, n)
		assert.Equal(t, name, v.(*Org).Name)
	})

	t.Run("with pointer bind", func(t *testing.T) {
		registry := eventsource.New(Org{})
		registry.BindFunc(&OrgNameSet{}, SetOrgName)

		err := registry.Save(ctx, OrgNameSet{
			Model: eventsource.Model{ID: aggregateID, Version: 0},
			Name:  name,
		})
		assert.Nil(t, err)

		v, n, err := registry.Load(ctx, aggregateID)
		assert.Nil(t, err)
		assert.Equal(t, 0, n)
		assert.Equal(t, name, v.(*Org).Name)
	})
}
