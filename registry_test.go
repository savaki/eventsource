package eventsource_test

import (
	"context"
	"os"
	"testing"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type Org struct {
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

func CreateOrg(_ context.Context, aggregate, event interface{}) error {
	e := event.(*OrgCreated)
	org := aggregate.(*Org)
	org.ID = e.Model.ID
	org.CreatedAt = e.Model.At
	org.UpdatedAt = e.Model.At
	return nil
}

func SetOrgName(_ context.Context, aggregate, event interface{}) error {
	e := event.(*OrgNameSet)
	org := aggregate.(*Org)
	org.Name = e.Name
	org.UpdatedAt = e.Model.At
	return nil
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()
	id := "123"
	name := "Jones"

	t.Run("simple", func(t *testing.T) {
		registry := eventsource.New(Org{}, eventsource.WithDebug(os.Stdout))
		registry.BindFunc(OrgCreated{}, CreateOrg)
		registry.BindFunc(OrgNameSet{}, SetOrgName)

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

		v, version, err := registry.Load(ctx, id, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, version)

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

		v, version, err = registry.Load(ctx, id, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, version)

		org, ok = v.(*Org)
		assert.True(t, ok)
		assert.Equal(t, id, org.ID)
		assert.Equal(t, updated, org.Name)
	})

	t.Run("with pointer prototype", func(t *testing.T) {
		registry := eventsource.New(&Org{})
		registry.BindFunc(OrgCreated{}, CreateOrg)
		registry.BindFunc(OrgNameSet{}, SetOrgName)

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

		v, version, err := registry.Load(ctx, id, 0)
		assert.Nil(t, err)
		assert.Equal(t, 1, version)
		assert.Equal(t, name, v.(*Org).Name)
	})

	t.Run("with pointer bind", func(t *testing.T) {
		registry := eventsource.New(Org{})
		registry.BindFunc(&OrgNameSet{}, SetOrgName)

		err := registry.Save(ctx,
			&OrgNameSet{
				Model: eventsource.Model{ID: id, Version: 0},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, version, err := registry.Load(ctx, id, 0)
		assert.Nil(t, err)
		assert.Equal(t, 0, version)
		assert.Equal(t, name, v.(*Org).Name)
	})
}

func TestAt(t *testing.T) {
	ctx := context.Background()
	id := "123"

	registry := eventsource.New(Org{}, eventsource.WithDebug(os.Stdout))
	registry.BindFunc(OrgCreated{}, CreateOrg)
	err := registry.Save(ctx,
		&OrgCreated{
			Model: eventsource.Model{ID: id, Version: 1, At: eventsource.Now()},
		},
	)
	assert.Nil(t, err)

	v, version, err := registry.Load(ctx, id, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, version)

	org := v.(*Org)
	assert.NotZero(t, org.CreatedAt)
	assert.NotZero(t, org.UpdatedAt)
}
