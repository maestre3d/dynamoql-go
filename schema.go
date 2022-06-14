package dynamoql

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// DefaultSchemaField base attribute name for schema name field in an Amazon DynamoDB table.
// Used by query hydration mechanisms.
var DefaultSchemaField = "schema_name"

// Schema an Amazon DynamoDB model. Not necessarily a table.
type Schema interface {
	Marshaler
	Unmarshaler
}

// EdgeSchema an Amazon DynamoDB Many-To-Many model. Stores the keys from each side of the relation and
// additional attributes if desired.
type EdgeSchema interface {
	Schema
	GetLeftKeys() map[string]types.AttributeValue
	GetRightKeys() map[string]types.AttributeValue
}

// NodeSchema an Amazon DynamoDB entity. Not necessarily a table.
type NodeSchema interface {
	Schema
	GetName() string
	GetKeys() map[string]types.AttributeValue
}
