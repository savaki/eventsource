package dynamodbstore_test

import (
	"testing"

	"github.com/savaki/eventsource/provider/dynamodbstore"
	"github.com/stretchr/testify/assert"
)

func TestMakeCreateTableInput(t *testing.T) {
	expected := "new-hash-key"
	input := dynamodbstore.MakeCreateTableInput("blah", 3, 3, dynamodbstore.WithHashKey(expected))
	assert.Equal(t, expected, *input.AttributeDefinitions[0].AttributeName)
	assert.Equal(t, expected, *input.KeySchema[0].AttributeName)
}

func TestWithStreams(t *testing.T) {
	input := dynamodbstore.MakeCreateTableInput("blah", 3, 3, dynamodbstore.WithStreams())
	assert.NotNil(t, input.StreamSpecification)
	assert.True(t, *input.StreamSpecification.StreamEnabled)
	assert.Equal(t, "NEW_AND_OLD_IMAGES", *input.StreamSpecification.StreamViewType)
}
