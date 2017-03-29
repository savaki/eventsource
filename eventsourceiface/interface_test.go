package eventsourceiface_test

import (
	"testing"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/eventsourceiface"
	"github.com/stretchr/testify/assert"
)

type Aggregate struct {
	eventsource.Aggregate
}

type Sample struct {
	eventsourceiface.RepositoryAPI
}

func (s Sample) New() eventsource.Aggregate {
	return Aggregate{}
}

func TestEmbed(t *testing.T) {
	s := Sample{}
	assert.NotNil(t, s)
	assert.NotNil(t, s.New())
}
