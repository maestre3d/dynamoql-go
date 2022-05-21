package dynamoql

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const defaultLimit = 10

type Condition struct {
	Negate, IsKey bool
	Operator      ConditionalOperator
	// Used by Size operator only
	SecondaryOperator ConditionalOperator
	Field             string
	Value             interface{}
	// Used by Between and In operators only
	ExtraValues []interface{}
}

type QueryBuilder struct {
	isConsistent              bool
	negate                    bool
	limit                     int32
	operator                  LogicalOperator
	ordering                  Ordering
	table                     string
	index                     *string
	projectedFieldsExpression *string
	returnMetrics             types.ReturnConsumedCapacity
	conditions                []Condition
	pageToken                 PageToken
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		negate:        false,
		limit:         defaultLimit,
		returnMetrics: types.ReturnConsumedCapacityNone,
	}
}

func Select(projectedFields ...string) *QueryBuilder {
	return NewQueryBuilder().Select(projectedFields)
}

func (q *QueryBuilder) Select(projectedFields []string) *QueryBuilder {
	if projectedFields != nil && len(projectedFields) > 0 {
		q.projectedFieldsExpression = aws.String(strings.Join(projectedFields, ","))
	}
	return q
}

func (q *QueryBuilder) From(table string) *QueryBuilder {
	q.table = table
	return q
}

func (q *QueryBuilder) Negate() *QueryBuilder {
	q.negate = true
	return q
}

func (q *QueryBuilder) Where(c ...Condition) *QueryBuilder {
	q.conditions = c
	return q
}

func (q *QueryBuilder) And() *QueryBuilder {
	q.operator = And
	return q
}

func (q *QueryBuilder) Or() *QueryBuilder {
	q.operator = Or
	return q
}

func (q *QueryBuilder) Limit(l int32) *QueryBuilder {
	q.limit = l
	return q
}

func (q *QueryBuilder) PageToken(t PageToken) *QueryBuilder {
	q.pageToken = t
	return q
}

func (q *QueryBuilder) OrderBy(o Ordering) *QueryBuilder {
	q.ordering = o
	return q
}

func (q *QueryBuilder) Index(i string) *QueryBuilder {
	if i != "" {
		q.index = &i
	}
	return q
}

func (q *QueryBuilder) StrongConsistency() *QueryBuilder {
	q.isConsistent = true
	return q
}

func (q *QueryBuilder) Metrics(v types.ReturnConsumedCapacity) *QueryBuilder {
	q.returnMetrics = v
	return q
}

func (q *QueryBuilder) ExecGet(ctx context.Context, c *dynamodb.Client) (dynamodb.GetItemOutput, error) {
	out, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		Key:                      buildExpressionValuesRaw(q.conditions),
		TableName:                &q.table,
		ConsistentRead:           &q.isConsistent,
		ExpressionAttributeNames: nil,
		ProjectionExpression:     q.projectedFieldsExpression,
		ReturnConsumedCapacity:   q.returnMetrics,
	})
	if err != nil {
		return dynamodb.GetItemOutput{}, err
	}
	return *out, nil
}

type QueryResponse struct {
	Items              []map[string]types.AttributeValue
	NextPageToken      PageToken
	Count              int32
	ConsumptionMetrics []types.ConsumedCapacity
}

func (q *QueryBuilder) newQueryInput() *dynamodb.QueryInput {
	selectOpt := types.SelectAllAttributes
	if q.projectedFieldsExpression != nil {
		selectOpt = types.SelectSpecificAttributes
	}
	builder := newExpression(q.operator, q.negate, q.conditions)
	return &dynamodb.QueryInput{
		TableName:                 &q.table,
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

func (q *QueryBuilder) ExecQuery(ctx context.Context, c *dynamodb.Client) (QueryResponse, error) {
	in := q.newQueryInput()
	// reduce overall buffer size by half to avoid unnecessary malloc if no further items are found.
	// If all items found, golang internals will grow slice's cap by twice when hitting the half-divided capacity,
	// reaching the original capacity required by the buffer.
	itemsBuf := make([]map[string]types.AttributeValue, 0, int(q.limit)/2)
	nextPage := q.pageToken
	var count int32 = 0
	consumedCapacityBuf := make([]types.ConsumedCapacity, 0)
	for len(itemsBuf) < int(q.limit) {
		in.ExclusiveStartKey = nextPage
		out, err := c.Query(ctx, in)
		if err != nil {
			return QueryResponse{}, err
		} else if out.Items == nil {
			break
		}
		itemsBuf = append(itemsBuf, out.Items...)
		count += out.Count
		if out.ConsumedCapacity != nil {
			consumedCapacityBuf = append(consumedCapacityBuf, *out.ConsumedCapacity)
		}
		if len(itemsBuf) < int(q.limit) {
			nextPage = out.LastEvaluatedKey
		}
		if nextPage == nil {
			break
		}
		in.Limit = aws.Int32(*in.Limit - int32(len(out.Items)))
	}
	return QueryResponse{
		Items:              itemsBuf,
		NextPageToken:      nextPage,
		Count:              count,
		ConsumptionMetrics: consumedCapacityBuf,
	}, nil
}

func (q *QueryBuilder) ExecRawQuery(ctx context.Context, c *dynamodb.Client) (dynamodb.QueryOutput, error) {
	out, err := c.Query(ctx, q.newQueryInput())
	if err != nil {
		return dynamodb.QueryOutput{}, err
	}
	return *out, nil
}
