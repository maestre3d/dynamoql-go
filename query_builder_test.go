package dynamoql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueryBuilder(t *testing.T) {
	pageToken := dynamoql.NewPageTokenString("U351c2VyX2lkfjEyMy1hYmMmTn50aW1lc3RhbXBfdW5peH4xMjM=")
	b := dynamoql.Select("foo").
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
		PageToken(pageToken).
		StrongConsistency().
		OrderBy(dynamoql.Descend).
		Limit(10).
		DegreeOfParallelism(25).
		Metrics(types.ReturnConsumedCapacityIndexes)
	out := dynamoql.NewScanInput(b)
	require.NotEmpty(t, out)
	assert.Equal(t, "sample", *out.TableName)
	assert.Equal(t, "sample-gsi", *out.IndexName)
	assert.True(t, *out.ConsistentRead)
	assert.Equal(t, int32(25), *out.TotalSegments)
	assert.Equal(t, int32(10), *out.Limit)
	assert.EqualValues(t, pageToken, dynamoql.PageToken(out.ExclusiveStartKey))
	outQuery := dynamoql.NewQueryInput(b)
	assert.False(t, *outQuery.ScanIndexForward)
}
