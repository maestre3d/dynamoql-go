package dynamoql

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Marshaler converts an Amazon DynamoDB model into a primitive map.
type Marshaler interface {
	MarshalDynamoDB() (map[string]types.AttributeValue, error)
}

// Unmarshaler converts a primitive map into an Amazon DynamoDB model.
type Unmarshaler interface {
	UnmarshalDynamoDB(map[string]types.AttributeValue) error
}
