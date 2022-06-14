package dynamoql

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

var DefaultSchemaField = "schema_name"

type Schema interface {
	Marshaler
	Unmarshaler
}

type EdgeSchema interface {
	Schema
	GetLeftKeys() map[string]types.AttributeValue
	GetRightKeys() map[string]types.AttributeValue
}

type NodeSchema interface {
	Schema
	GetName() string
	GetKeys() map[string]types.AttributeValue
}
