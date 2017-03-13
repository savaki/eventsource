package dynamodbstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/savaki/eventsource"
)

const (
	DefaultRegion   = "us-east-1"
	DefaultHashKey  = "key"
	DefaultRangeKey = "partition"
)

const (
	eventTypePrefix = "_t"
	dataPrefix      = "_d"
)

// Store represents a dynamodb backed eventsource.Store
type Store struct {
	region        string
	tableName     string
	hashKey       string
	rangeKey      string
	api           *dynamodb.DynamoDB
	useStreams    bool
	eventsPerItem int
}

// Save implements the eventsource.Store interface
func (s *Store) Save(ctx context.Context, serializer eventsource.Serializer, events ...interface{}) error {
	inputs, err := makeUpdateItemInput(s.tableName, s.hashKey, s.rangeKey, s.eventsPerItem, serializer, events...)
	if err != nil {
		return err
	}

	for _, input := range inputs {
		_, err := s.api.UpdateItem(input)
		if err != nil {
			if v, ok := err.(awserr.Error); ok {
				return errors.Wrapf(err, "Save failed. %v [%v]", v.Message(), v.Code())
			}
			return err
		}
	}

	return nil
}

func (s *Store) Fetch(ctx context.Context, serializer eventsource.Serializer, aggregateID string, version int) ([]interface{}, int, error) {
	partition := selectPartition(version, s.eventsPerItem)
	input, err := makeQueryInput(s.tableName, s.hashKey, s.rangeKey, aggregateID, partition)
	if err != nil {
		return nil, 0, err
	}

	metas := make([]eventsource.EventMeta, 0, version)

	var startKey map[string]*dynamodb.AttributeValue
	for {
		out, err := s.api.Query(input)
		if err != nil {
			return nil, 0, err
		}

		if len(out.Items) == 0 {
			break
		}

		// events are stored within av as _t{version} = {event-type}, _d{version} = {serialized event}
		for _, item := range out.Items {
			for key, av := range item {
				if !strings.HasPrefix(key, eventTypePrefix) {
					continue
				}

				if version > 0 {
					if v, err := strconv.Atoi(key[len(eventTypePrefix):]); err != nil || v > version {
						continue
					}
				}

				eventType := *av.S

				dataKey := dataPrefix + key[len(eventTypePrefix):]
				data := item[dataKey].B
				event, err := serializer.Deserialize(eventType, data)
				if err != nil {
					return nil, 0, err
				}

				meta, err := eventsource.Inspect(event)
				if err != nil {
					return nil, 0, err
				}

				metas = append(metas, meta)
			}
		}

		startKey = out.LastEvaluatedKey
		if len(startKey) == 0 {
			break
		}
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Version < metas[j].Version
	})

	foundVersion := 0
	events := make([]interface{}, 0, version)
	for _, meta := range metas {
		events = append(events, meta.Event)
		foundVersion = meta.Version
	}

	return events, foundVersion, nil
}

func New(tableName string, opts ...Option) (*Store, error) {
	store := &Store{
		region:        DefaultRegion,
		tableName:     tableName,
		hashKey:       DefaultHashKey,
		rangeKey:      DefaultRangeKey,
		eventsPerItem: 1,
	}

	for _, opt := range opts {
		opt(store)
	}

	if store.api == nil {
		cfg := &aws.Config{Region: aws.String(store.region)}
		s, err := session.NewSession(cfg)
		if err != nil {
			if v, ok := err.(awserr.Error); ok {
				return nil, errors.Wrapf(err, "Unable to create AWS Session - %v [%v]", v.Message(), v.Code())
			}
			return nil, err
		}
		store.api = dynamodb.New(s)
	}

	return store, nil
}

func partition(eventsPerItem int, events ...interface{}) (map[int][]eventsource.EventMeta, error) {
	partitions := map[int][]eventsource.EventMeta{}

	for _, event := range events {
		meta, err := eventsource.Inspect(event)
		if err != nil {
			return nil, err
		}

		id := selectPartition(meta.Version, eventsPerItem)
		p, ok := partitions[id]
		if !ok {
			p = []eventsource.EventMeta{}
		}

		partitions[id] = append(p, meta)
	}

	return partitions, nil
}

func makeUpdateItemInput(tableName, hashKey, rangeKey string, eventsPerItem int, serializer eventsource.Serializer, events ...interface{}) ([]*dynamodb.UpdateItemInput, error) {
	eventCount := len(events)
	partitions, err := partition(eventsPerItem, events...)
	if err != nil {
		return nil, err
	}

	inputs := make([]*dynamodb.UpdateItemInput, 0, eventCount)
	for partitionID, partition := range partitions {
		input := &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]*dynamodb.AttributeValue{
				hashKey:  {S: aws.String(partition[0].AggregateID)},
				rangeKey: {N: aws.String(strconv.Itoa(partitionID))},
			},
			ExpressionAttributeNames: map[string]*string{
				"#revision": aws.String("revision"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":one": {N: aws.String("1")},
			},
		}

		// Add each element within the partition to the UpdateItemInput

		condExpr := &bytes.Buffer{}
		updateExpr := &bytes.Buffer{}
		io.WriteString(updateExpr, "ADD #revision :one SET ")

		for index, meta := range partition {
			version := strconv.Itoa(meta.Version)

			// Each event is store as two entries, an event entries and an event type entry.

			// Store the event itself
			{
				data, err := serializer.Serialize(meta.Event)
				if err != nil {
					return nil, err
				}

				key := dataPrefix + version
				nameRef := "#" + dataPrefix + version
				valueRef := ":" + dataPrefix + version

				if index > 0 {
					io.WriteString(condExpr, " AND ")
					io.WriteString(updateExpr, ", ")
				}
				fmt.Fprintf(condExpr, "attribute_not_exists(%v)", nameRef)
				fmt.Fprintf(updateExpr, "%v = %v", nameRef, valueRef)
				input.ExpressionAttributeNames[nameRef] = aws.String(key)
				input.ExpressionAttributeValues[valueRef] = &dynamodb.AttributeValue{B: data}
			}

			// Store the event type
			{

				key := eventTypePrefix + version
				nameRef := "#" + eventTypePrefix + version
				valueRef := ":" + eventTypePrefix + version
				fmt.Fprintf(updateExpr, ", %v = %v", nameRef, valueRef)
				input.ExpressionAttributeNames[nameRef] = aws.String(key)
				input.ExpressionAttributeValues[valueRef] = &dynamodb.AttributeValue{S: aws.String(meta.EventType)}
			}
		}

		input.ConditionExpression = aws.String(condExpr.String())
		input.UpdateExpression = aws.String(updateExpr.String())

		inputs = append(inputs, input)
	}

	return inputs, nil
}

// makeQueryInput
//  - partition - fetch up to this partition number; 0 to fetch all partitions
func makeQueryInput(tableName, hashKey, rangeKey string, aggregateID string, partition int) (*dynamodb.QueryInput, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(tableName),
		Select:         aws.String("ALL_ATTRIBUTES"),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#key": aws.String(hashKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": {S: aws.String(aggregateID)},
		},
	}

	if partition == 0 {
		input.KeyConditionExpression = aws.String("#key = :key")

	} else {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition <= :partition")
		input.ExpressionAttributeNames["#partition"] = aws.String(rangeKey)
		input.ExpressionAttributeValues[":partition"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(partition))}
	}

	return input, nil
}

func selectPartition(version, eventsPerItem int) int {
	return version / eventsPerItem
}
