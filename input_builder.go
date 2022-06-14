package dynamoql

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewQueryInput builds a dynamodb.QueryInput using current QueryBuilder instance values.
func NewQueryInput(q *QueryBuilder) dynamodb.QueryInput {
	selectOpt := types.SelectAllAttributes
	if q.projectedFieldsExpression != nil {
		selectOpt = types.SelectSpecificAttributes
	}
	builder := newExpression(q.operator, q.negate, q.conditions)
	return dynamodb.QueryInput{
		TableName:                 &q.table,
		ExclusiveStartKey:         q.pageToken,
		ConsistentRead:            &q.isConsistent,
		ExpressionAttributeNames:  builder.Names,
		ExpressionAttributeValues: builder.Values,
		FilterExpression:          builder.FilterExpression,
		IndexName:                 q.index,
		KeyConditionExpression:    builder.KeyExpression,
		Limit:                     &q.limit,
		ProjectionExpression:      q.projectedFieldsExpression,
		ReturnConsumedCapacity:    q.returnMetrics,
		ScanIndexForward:          aws.Bool(q.ordering == Ascend),
		Select:                    selectOpt,
	}
}

// NewScanInput builds a dynamodb.ScanInput using current QueryBuilder instance values.
func NewScanInput(q *QueryBuilder) dynamodb.ScanInput {
	selectOpt := types.SelectAllAttributes
	if q.projectedFieldsExpression != nil {
		selectOpt = types.SelectSpecificAttributes
	}
	builder := newExpression(q.operator, q.negate, q.conditions)
	return dynamodb.ScanInput{
		TableName:                 &q.table,
		ConsistentRead:            &q.isConsistent,
		ExclusiveStartKey:         q.pageToken,
		ExpressionAttributeNames:  builder.Names,
		ExpressionAttributeValues: builder.Values,
		FilterExpression:          builder.FilterExpression,
		IndexName:                 q.index,
		Limit:                     &q.limit,
		ProjectionExpression:      q.projectedFieldsExpression,
		ReturnConsumedCapacity:    q.returnMetrics,
		Segment:                   nil,
		Select:                    selectOpt,
		TotalSegments:             &q.parallelDegree,
	}
}

// NewGetInput builds a dynamodb.GetItemInput using current QueryBuilder instance values.
func NewGetInput(q *QueryBuilder) dynamodb.GetItemInput {
	return dynamodb.GetItemInput{
		Key:                      buildExpressionValuesRaw(q.conditions),
		TableName:                &q.table,
		ConsistentRead:           &q.isConsistent,
		ExpressionAttributeNames: nil,
		ProjectionExpression:     q.projectedFieldsExpression,
		ReturnConsumedCapacity:   q.returnMetrics,
	}
}
