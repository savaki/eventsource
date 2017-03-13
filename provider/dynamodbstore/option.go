package dynamodbstore

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Option func(*Store)

// WithRegion specifies the AWS Region to connect to
func WithRegion(region string) Option {
	return func(s *Store) {
		s.region = region
	}
}

// WithHashKey specifies the alternate hash key to use
func WithHashKey(hashKey string) Option {
	return func(s *Store) {
		s.hashKey = hashKey
	}
}

// WithRangeKey specifies the alternate range key (sort key) to use
func WithRangeKey(rangeKey string) Option {
	return func(s *Store) {
		s.rangeKey = rangeKey
	}
}

// WithEventPerItem allows you to specify the number of events to be stored per dynamodb record; defaults to 1
func WithEventPerItem(eventsPerItem int) Option {
	return func(s *Store) {
		s.eventsPerItem = eventsPerItem
	}
}

// WithDynamoDB allows the caller to specify a pre-configured reference to DynamoDB
func WithDynamoDB(api *dynamodb.DynamoDB) Option {
	return func(s *Store) {
		s.api = api
	}
}

// WithStreams is an option only used by the MakeCreateTableInput that indicates the table should be created with
// DynamoDB streams enabled
func WithStreams() Option {
	return func(s *Store) {
		s.useStreams = true
	}
}
