package providertest

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type EntitySetFirst struct {
	eventsource.Model
	First string
}

type EntitySetLast struct {
	eventsource.Model
	Last string
}

func TestStore_Save(t *testing.T, store eventsource.Store) {
	aggregateID := strconv.FormatInt(time.Now().UnixNano(), 10)
	e1 := EntitySetFirst{
		Model: eventsource.Model{
			ID:      aggregateID,
			Version: 1,
		},
		First: "first",
	}
	e2 := EntitySetLast{
		Model: eventsource.Model{
			ID:      aggregateID,
			Version: 2,
		},
		Last: "last",
	}

	serializer := eventsource.JSONSerializer()
	serializer.Bind(e1, e2)

	r1, err := serializer.Serialize(e1)
	assert.Nil(t, err)

	r2, err := serializer.Serialize(e2)
	assert.Nil(t, err)

	err = store.Save(context.Background(), e1.Model.ID, r1, r2)
	assert.Nil(t, err)

	history, err := store.Fetch(context.Background(), aggregateID, 0)
	assert.Nil(t, err)
	assert.Equal(t, eventsource.History{r1, r2}, history)
	assert.Equal(t, e2.Model.Version, history[1].Version)
}
