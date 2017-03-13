package dynamodbstore_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/provider/dynamodbstore"
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

func TestSave(t *testing.T) {
	aggregateID := strconv.FormatInt(time.Now().UnixNano(), 10)
	first := EntitySetFirst{
		Model: eventsource.Model{
			AggregateID: aggregateID,
			Version:     1,
		},
		First: "first",
	}
	second := EntitySetLast{
		Model: eventsource.Model{
			AggregateID: aggregateID,
			Version:     2,
		},
		Last: "last",
	}

	serializer := eventsource.JSONSerializer()
	serializer.Bind(first, second)

	ctx := context.Background()
	store, err := dynamodbstore.New("sample_events", dynamodbstore.WithEventPerItem(12))
	assert.Nil(t, err)

	err = store.Save(ctx, serializer, first, second)
	assert.Nil(t, err)

	events, err := store.Fetch(ctx, serializer, aggregateID, 0)
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{&first, &second}, events)
}
