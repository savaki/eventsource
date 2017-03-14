package dynamodbstore_test

import (
	"context"
	"log"
	"os"
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

var api *dynamodb.DynamoDB

func init() {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentials("blah", "blah", ""),
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String("http://localhost:8000"),
	}
	s, err := session.NewSession(cfg)
	if err != nil {
		log.Fatalln(err)
	}
	api = dynamodb.New(s)
}

type EntitySetFirst struct {
	eventsource.Model
	First string
}

type EntitySetLast struct {
	eventsource.Model
	Last string
}

func TestIsKey(t *testing.T) {
	testCases := map[string]struct {
		Key      string
		Expected bool
	}{
		"simple": {
			Key:      "_d1",
			Expected: true,
		},
		"failed": {
			Key:      "d1",
			Expected: false,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			assert.Equal(t, tc.Expected, dynamodbstore.IsKey(tc.Key))
		})
	}
}

func TestVersionFromKey(t *testing.T) {
	testCases := map[string]struct {
		Key     string
		Version int
		HasErr  bool
	}{
		"simple": {
			Key:     "_d1",
			Version: 1,
		},
		"larger": {
			Key:     "_d1234",
			Version: 1234,
		},
		"empty": {
			Key:    "_d",
			HasErr: true,
		},
		"bad": {
			Key:    "_dabc",
			HasErr: true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			version, err := dynamodbstore.VersionFromKey(tc.Key)
			assert.True(t, tc.HasErr == (err != nil))
			assert.Equal(t, tc.Version, version)
		})
	}
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
	tableName := "sample_events"
	_, err := api.CreateTable(dynamodbstore.MakeCreateTableInput(tableName, 10, 10))
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
					ID:      aggregateID,
					Version: 1,
				},
				First: "first",
			}
			second := EntitySetLast{
				Model: eventsource.Model{
					ID:      aggregateID,
					Version: 2,
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

			history, err := store.Fetch(ctx, serializer, aggregateID, 0)
			assert.Nil(t, err)
			assert.Equal(t, []interface{}{&first, &second}, history.Events)
			assert.Equal(t, second.Model.Version, history.Version)

			partitions, err := fetchPartitions(api, tableName, aggregateID)
			assert.Nil(t, err)
			assert.Equal(t, tc.Partitions, partitions)
		})
	}
}

func TestStore_Fetch(t *testing.T) {
	tableName := "sample_events"

	aggregateID := strconv.FormatInt(time.Now().UnixNano(), 10)
	first := EntitySetFirst{
		Model: eventsource.Model{
			ID:      aggregateID,
			Version: 1,
		},
		First: "first",
	}
	second := EntitySetLast{
		Model: eventsource.Model{
			ID:      aggregateID,
			Version: 2,
		},
		Last: "last",
	}

	serializer := eventsource.JSONSerializer()
	serializer.Bind(first, second)

	store, err := dynamodbstore.New(tableName,
		dynamodbstore.WithDynamoDB(api),
		dynamodbstore.WithDebug(os.Stdout),
	)
	assert.Nil(t, err)

	ctx := context.Background()
	err = store.Save(ctx, serializer, first, second)
	assert.Nil(t, err)

	history, err := store.Fetch(ctx, serializer, aggregateID, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, history.Version)
	assert.Equal(t, 1, len(history.Events))
	assert.Equal(t, &first, history.Events[0])
}
