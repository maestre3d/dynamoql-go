package dynamoql

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Marshaler interface {
	MarshalDynamoDB() (map[string]types.AttributeValue, error)
}

type Unmarshaler interface {
	UnmarshalDynamoDB(map[string]types.AttributeValue) error
}
