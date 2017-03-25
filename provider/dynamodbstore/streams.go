package dynamodbstore

import (
	"errors"
	"sort"
	"strings"

	"github.com/apex/go-apex/dynamo"
)

type event struct {
	Version int
	Data    []byte
}

// Changes returns an ordered list of changes from the *dynamo.Record; will never return nil
func RawEvents(record *dynamo.Record) ([][]byte, error) {
	keys := map[string]struct{}{}

	// determine which keys are new

	if record != nil && record.Dynamodb != nil {
		if record.Dynamodb.NewImage != nil {
			for k := range record.Dynamodb.NewImage {
				if IsKey(k) {
					keys[k] = struct{}{}
				}
			}
		}

		if record.Dynamodb.OldImage != nil {
			for k := range record.Dynamodb.OldImage {
				if IsKey(k) {
					delete(keys, k)
				}
			}
		}
	}

	// using those keys, construct a sorted list of items

	items := make([]event, 0, len(keys))
	for key := range keys {
		version, err := VersionFromKey(key)
		if err != nil {
			return nil, err
		}

		data := record.Dynamodb.NewImage[key].B

		items = append(items, event{
			Version: version,
			Data:    data,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Version < items[j].Version
	})

	events := make([][]byte, 0, len(items))
	for _, item := range items {
		events = append(events, item.Data)
	}

	return events, nil
}

var (
	errInvalidEventSource = errors.New("invalid event source arn")
)

// TableName extracts a table name from a dynamodb event source arn
// arn:aws:dynamodb:us-west-2:528688496454:table/dealerflare-local-orgs/stream/2017-03-14T04:49:34.930
func TableName(eventSource string) (string, error) {
	segments := strings.Split(eventSource, "/")
	if len(segments) < 2 {
		return "", errInvalidEventSource
	}

	return segments[1], nil
}
