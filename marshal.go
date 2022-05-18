package dynamoql

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/modern-go/reflect2"
)

const dynamodbStructTag = "dynamodb"

type Marshaler interface {
	MarshalDynamoDB() (map[string]types.AttributeValue, error)
}

type Unmarshaler interface {
	UnmarshalDynamoDB(map[string]types.AttributeValue) error
}

func Marshal(v interface{}) (map[string]types.AttributeValue, error) {
	typeOf := reflect2.TypeOf(v)
	if typeOf.Kind() != reflect.Struct {
		return nil, errors.New("not a valid struct")
	}
	buff := map[string]types.AttributeValue{}
	return buff, nil
}

func Unmarshal(data map[string]types.AttributeValue, v interface{}) error {
	return nil
}
