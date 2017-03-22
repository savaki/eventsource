package eventsource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Embedded struct {
	Model
}

type CustomModel struct {
	ID      string      `eventsource:"id"`
	Version int         `eventsource:"version"`
	At      EpochMillis `eventsource:"at"`
}

type EmbeddedCustom struct {
	CustomModel
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
				ID:      "123",
				Version: 4,
				At:      567,
			},
		}
		meta, err := Inspect(item)
		assert.Nil(t, err)
		assert.Equal(t, item.Model.ID, meta.ID)
		assert.Equal(t, "Embedded", meta.EventType)
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
		assert.Equal(t, item.ID, meta.ID)
		assert.Equal(t, "blah", meta.EventType)
		assert.Equal(t, item.Revision, meta.Version)
		assert.Equal(t, item.CreatedAt.Unix(), meta.At.Time().Unix())
	})

	t.Run("custom", func(t *testing.T) {
		item := EmbeddedCustom{
			CustomModel: CustomModel{
				ID:      "123",
				Version: 4,
				At:      567,
			},
		}
		meta, err := Inspect(item)
		assert.Nil(t, err)
		assert.Equal(t, item.ID, meta.ID)
		assert.Equal(t, "EmbeddedCustom", meta.EventType)
		assert.Equal(t, item.Version, meta.Version)
		assert.Equal(t, item.At, meta.At)
	})
}

var (
	meta EventMeta
)

func BenchmarkInspectEmbedded(b *testing.B) {
	item := Embedded{
		Model: Model{
			ID: "123",
		},
	}

	for i := 0; i < b.N; i++ {
		meta, _ = Inspect(item)
	}

	assert.Equal(b, item.Model.ID, meta.ID)
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

	assert.Equal(b, item.ID, meta.ID)
}

func TestTime(t *testing.T) {
	now := time.Now()
	epoch := Time(now)
	assert.Equal(t, now.Format(time.StampMilli), epoch.Time().Format(time.StampMilli))
}

func TestNow(t *testing.T) {
	delta := Now().Time().Sub(time.Now())
	if delta < 0 {
		delta *= -1
	}
	assert.True(t, delta <= time.Millisecond)
}
