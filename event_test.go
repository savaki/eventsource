package eventsource_test

import (
	"testing"
	"time"

	"github.com/savaki/eventsource"
	"github.com/stretchr/testify/assert"
)

type Embedded struct {
	eventsource.Model
}

type CustomModel struct {
	ID      string
	Version int
	At      time.Time
}

func (m CustomModel) AggregateID() string {
	return m.ID
}

func (m CustomModel) EventVersion() int {
	return m.Version
}

func (m CustomModel) EventAt() time.Time {
	return m.At
}

type EmbeddedCustom struct {
	CustomModel
}

var (
	event eventsource.Event
)

func BenchmarkInspectEmbedded(b *testing.B) {
	id := "123"
	var item interface{} = Embedded{
		Model: eventsource.Model{
			ID: id,
		},
	}

	for i := 0; i < b.N; i++ {
		event, _ = item.(eventsource.Event)
	}

	assert.Equal(b, id, event.AggregateID())
}
