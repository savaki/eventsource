package eventsource_test

import (
	"context"
	"os"
	"testing"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type Org struct {
	Version   int
	ID        string
	Name      string
	CreatedAt eventsource.EpochMillis
	UpdatedAt eventsource.EpochMillis
}

type OrgCreated struct {
	eventsource.Model
	ID string
}

type OrgNameSet struct {
	eventsource.Model
	Name string
}

func (item *Org) Apply(event interface{}) bool {
	switch v := event.(type) {
	case *OrgCreated:
		item.Version = v.Model.Version
		item.ID = v.Model.ID
		item.CreatedAt = v.Model.At
		item.UpdatedAt = v.Model.At

	case *OrgNameSet:
		item.Version = v.Model.Version
		item.Name = v.Name
		item.UpdatedAt = v.Model.At

	default:
		return false
	}

	return true
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()
	id := "123"
	name := "Jones"

	t.Run("simple", func(t *testing.T) {
		registry := eventsource.New(&Org{}, eventsource.WithDebug(os.Stdout))
		registry.Bind(OrgCreated{})
		registry.Bind(OrgNameSet{})

		// Test - Add an event to the store and verify we can recreate the object

		err := registry.Save(ctx,
			&OrgCreated{
				Model: eventsource.Model{ID: id, Version: 0},
			},
			&OrgNameSet{
				Model: eventsource.Model{ID: id, Version: 1},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := registry.Load(ctx, id)
		assert.Nil(t, err)

		org, ok := v.(*Org)
		assert.True(t, ok)
		assert.Equal(t, id, org.ID)
		assert.Equal(t, name, org.Name)

		// Test - Update the org name and verify that the change is reflected in the loaded result

		updated := "Sarah"
		err = registry.Save(ctx, &OrgNameSet{
			Model: eventsource.Model{ID: id, Version: 2},
			Name:  updated,
		})
		assert.Nil(t, err)

		v, err = registry.Load(ctx, id)
		assert.Nil(t, err)

		org, ok = v.(*Org)
		assert.True(t, ok)
		assert.Equal(t, id, org.ID)
		assert.Equal(t, updated, org.Name)
	})

	t.Run("with pointer prototype", func(t *testing.T) {
		registry := eventsource.New(&Org{})
		registry.Bind(OrgCreated{})
		registry.Bind(OrgNameSet{})

		err := registry.Save(ctx,
			&OrgCreated{
				Model: eventsource.Model{ID: id, Version: 0},
			},
			&OrgNameSet{
				Model: eventsource.Model{ID: id, Version: 1},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := registry.Load(ctx, id)
		assert.Nil(t, err)
		assert.Equal(t, name, v.(*Org).Name)
	})

	t.Run("with pointer bind", func(t *testing.T) {
		registry := eventsource.New(&Org{})
		registry.Bind(&OrgNameSet{})

		err := registry.Save(ctx,
			&OrgNameSet{
				Model: eventsource.Model{ID: id, Version: 0},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := registry.Load(ctx, id)
		assert.Nil(t, err)
		assert.Equal(t, name, v.(*Org).Name)
	})
}

func TestAt(t *testing.T) {
	ctx := context.Background()
	id := "123"

	registry := eventsource.New(&Org{}, eventsource.WithDebug(os.Stdout))
	registry.Bind(OrgCreated{})
	err := registry.Save(ctx,
		&OrgCreated{
			Model: eventsource.Model{ID: id, Version: 1, At: eventsource.Now()},
		},
	)
	assert.Nil(t, err)

	v, err := registry.Load(ctx, id)
	assert.Nil(t, err)

	org := v.(*Org)
	assert.NotZero(t, org.CreatedAt)
	assert.NotZero(t, org.UpdatedAt)
}
