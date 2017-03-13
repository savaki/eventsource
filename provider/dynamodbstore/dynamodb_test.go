package dynamodbstore_test

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func fetchPartitions(api *dynamodb.DynamoDB, tableName, key string) ([]string, error) {
	var startKey map[string]*dynamodb.AttributeValue

	partitions := []string{}
	for {
		out, err := api.Query(&dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("#key = :key"),
			ExpressionAttributeNames: map[string]*string{
				"#key": aws.String("key"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":key": {S: aws.String(key)},
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}

		if len(out.Items) == 0 {
			break
		}

		for _, item := range out.Items {
			partitions = append(partitions, *item["partition"].N)
		}

		startKey = out.LastEvaluatedKey
		if len(startKey) == 0 {
			break
		}
	}

	sort.Strings(partitions)

	return partitions, nil
}

func TestSave(t *testing.T) {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentials("blah", "blah", ""),
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String("http://localhost:8001"),
	}
	s, err := session.NewSession(cfg)
	assert.Nil(t, err)
	api := dynamodb.New(s)

	tableName := "sample_events"
	_, err = api.CreateTable(dynamodbstore.MakeCreateTableInput(tableName, 10, 10))
	if err != nil {
		v, ok := err.(awserr.Error)
		assert.True(t, ok && v.Code() == "ResourceInUseException")
	}

	testCases := map[string]struct {
		EventsPerItem int
		Partitions    []string
	}{
		"big": {
			EventsPerItem: 3,
			Partitions:    []string{"0"},
		},
		"split": {
			EventsPerItem: 2,
			Partitions:    []string{"0", "1"},
		},
		"single": {
			EventsPerItem: 1,
			Partitions:    []string{"1", "2"},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
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
			store, err := dynamodbstore.New(tableName,
				dynamodbstore.WithEventPerItem(tc.EventsPerItem),
				dynamodbstore.WithDynamoDB(api),
			)
			assert.Nil(t, err)

			err = store.Save(ctx, serializer, first, second)
			assert.Nil(t, err)

			events, version, err := store.Fetch(ctx, serializer, aggregateID, 0)
			assert.Nil(t, err)
			assert.Equal(t, []interface{}{&first, &second}, events)
			assert.Equal(t, second.Model.Version, version)

			partitions, err := fetchPartitions(api, tableName, aggregateID)
			assert.Nil(t, err)
			assert.Equal(t, tc.Partitions, partitions)
		})
	}
}
