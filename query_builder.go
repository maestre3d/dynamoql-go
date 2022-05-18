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
	Negate   bool
	Operator ConditionalOperator
	Key      string
	Value    interface{}
}

func marshalDynamoDBCondList(c []Condition) map[string]types.AttributeValue {
	if c == nil || len(c) == 0 {
		return map[string]types.AttributeValue{}
	}
	resBuf := make(map[string]types.AttributeValue, len(c))
	for i := range c {
		resBuf[c[i].Key] = FormatAttribute(c[i].Value)
	}
	return resBuf
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

	conditions []Condition

	pageToken PageToken
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

func (q *QueryBuilder) WithIndex(i string) *QueryBuilder {
	if i != "" {
		q.index = &i
	}
	return q
}

func (q *QueryBuilder) WithStrongConsistency() *QueryBuilder {
	q.isConsistent = true
	return q
}

func (q *QueryBuilder) WithMetrics(v types.ReturnConsumedCapacity) *QueryBuilder {
	q.returnMetrics = v
	return q
}

func (q *QueryBuilder) Get(ctx context.Context, c *dynamodb.Client) (dynamodb.GetItemOutput, error) {
	out, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		Key:                      marshalDynamoDBCondList(q.conditions),
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

func (q *QueryBuilder) Query(ctx context.Context, c *dynamodb.Client) (QueryResponse, error) {
	selectOpt := types.SelectAllAttributes
	if q.projectedFieldsExpression != nil {
		selectOpt = types.SelectSpecificAttributes
	}
	in := &dynamodb.QueryInput{
		TableName:                 &q.table,
		ConsistentRead:            &q.isConsistent,
		ExpressionAttributeNames:  nil,
		ExpressionAttributeValues: nil,
		FilterExpression:          nil,
		IndexName:                 q.index,
		KeyConditionExpression:    nil,
		Limit:                     &q.limit,
		ProjectionExpression:      q.projectedFieldsExpression,
		ReturnConsumedCapacity:    q.returnMetrics,
		ScanIndexForward:          aws.Bool(q.ordering == Ascend),
		Select:                    selectOpt,
	}

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
		count = out.Count
		nextPage = out.LastEvaluatedKey
		if out.ConsumedCapacity != nil {
			consumedCapacityBuf = append(consumedCapacityBuf, *out.ConsumedCapacity)
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
