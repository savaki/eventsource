package dynamodbstore

import (
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

var api *dynamodb.DynamoDB

func init() {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentials("blah", "blah", ""),
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String("http://localhost:8001"),
	}
	s, err := session.NewSession(cfg)
	if err != nil {
		log.Fatalln(err)
	}
	api = dynamodb.New(s)
}

func TestWithHashKey(t *testing.T) {
	expected := "das-hash-key"
	s, err := New("blah", WithHashKey(expected), WithDynamoDB(api))
	assert.Nil(t, err)
	assert.Equal(t, expected, s.hashKey)
}

func TestWithRangeKey(t *testing.T) {
	expected := "das-range-key"
	s, err := New("blah", WithRangeKey(expected), WithDynamoDB(api))
	assert.Nil(t, err)
	assert.Equal(t, expected, s.rangeKey)
}

func TestWithRegion(t *testing.T) {
	expected := "das-region"
	s, err := New("blah", WithRegion(expected), WithDynamoDB(api))
	assert.Nil(t, err)
	assert.Equal(t, expected, s.region)
}
