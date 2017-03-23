package eventsource_test

import (
	"testing"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type EntitySetName struct {
	eventsource.Model
	Name string
}

func TestJSONSerializer(t *testing.T) {
	event := EntitySetName{
		Model: eventsource.Model{
			ID:      "123",
			Version: 456,
		},
		Name: "blah",
	}

	serializer := eventsource.JSONSerializer()
	serializer.Bind(event)
	record, err := serializer.Serialize(event)
	assert.Nil(t, err)
	assert.NotNil(t, record)

	v, err := serializer.Deserialize(record)
	assert.Nil(t, err)

	found, ok := v.(*EntitySetName)
	assert.True(t, ok)
	assert.Equal(t, &event, found)
}
