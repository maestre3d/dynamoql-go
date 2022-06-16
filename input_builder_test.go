package dynamoql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

func BenchmarkNewGetInput(b *testing.B) {
	builder := dynamoql.Select("foo").
		From("sample").
		Index("sample-gsi").
		And().
		Negate().
		Or(). // replaces And
		Where(dynamoql.Condition{
			Negate:            false,
			IsKey:             true,
			Operator:          dynamoql.Equals,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             "bar",
			ExtraValues:       nil,
		}).
		StrongConsistency().
		OrderBy(dynamoql.Descend).
		Limit(10).
		DegreeOfParallelism(25).
		Metrics(types.ReturnConsumedCapacityIndexes)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dynamoql.NewGetInput(builder)
	}
}
