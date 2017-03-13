package eventsource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Embedded struct {
	Model
}

type Tagged struct {
	ID        string    `eventsource:"id,type:blah"`
	Revision  int       `eventsource:"version"`
	CreatedAt time.Time `eventsource:"at"`
}

func TestInspect(t *testing.T) {
	t.Run("embbeded", func(t *testing.T) {
		item := Embedded{
			Model: Model{
				AggregateID: "123",
				Version:     4,
				At:          567,
			},
		}
		meta, err := Inspect(item)
		assert.Nil(t, err)
		assert.Equal(t, item.Model.AggregateID, meta.AggregateID)
		assert.Equal(t, "Embedded", meta.AggregateType)
		assert.Equal(t, item.Model.Version, meta.Version)
		assert.Equal(t, item.Model.At, meta.At)
	})

	t.Run("tagged", func(t *testing.T) {
		item := Tagged{
			ID:        "123",
			Revision:  4,
			CreatedAt: time.Now(),
		}
		meta, err := Inspect(item)
		assert.Nil(t, err)
		assert.Equal(t, item.ID, meta.AggregateID)
		assert.Equal(t, "blah", meta.AggregateType)
		assert.Equal(t, item.Revision, meta.Version)
		assert.Equal(t, item.CreatedAt.Unix(), meta.At.Time().Unix())
	})
}

var (
	meta EventMeta
)

func BenchmarkInspectEmbedded(b *testing.B) {
	item := Embedded{
		Model: Model{
			AggregateID: "123",
		},
	}

	for i := 0; i < b.N; i++ {
		meta, _ = Inspect(item)
	}

	assert.Equal(b, item.Model.AggregateID, meta.AggregateID)
}

func BenchmarkInspectTagged(b *testing.B) {
	item := Tagged{
		ID:        "123",
		Revision:  4,
		CreatedAt: time.Now(),
	}

	for i := 0; i < b.N; i++ {
		meta, _ = Inspect(item)
	}

	assert.Equal(b, item.ID, meta.AggregateID)
}

func TestTime(t *testing.T) {
	now := time.Now()
	epoch := Time(now)
	assert.Equal(t, now.Format(time.StampMilli), epoch.Time().Format(time.StampMilli))
}

func TestSetAggregateID(t *testing.T) {
	aggregateID := "123"

	t.Run("embedded", func(t *testing.T) {
		item := &Embedded{}
		setAggregateID(item, aggregateID)
		assert.Equal(t, aggregateID, item.Model.AggregateID)
	})

	t.Run("tagged", func(t *testing.T) {
		item := &Tagged{}
		setAggregateID(item, aggregateID)
		assert.Equal(t, aggregateID, item.ID)
	})
}
