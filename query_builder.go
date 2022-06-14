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
	parallelDegree            int32
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

// DegreeOfParallelism only for Scan operations.
func (q *QueryBuilder) DegreeOfParallelism(d int32) *QueryBuilder {
	q.parallelDegree = d
	return q
}

func (q *QueryBuilder) GetQueryInput() *dynamodb.QueryInput {
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

func (q *QueryBuilder) GetGetInput() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		Key:                      buildExpressionValuesRaw(q.conditions),
		TableName:                &q.table,
		ConsistentRead:           &q.isConsistent,
		ExpressionAttributeNames: nil,
		ProjectionExpression:     q.projectedFieldsExpression,
		ReturnConsumedCapacity:   q.returnMetrics,
	}
}

func (q *QueryBuilder) GetQueryPaginator(c *dynamodb.Client) *QueryPaginator {
	return NewQueryPaginator(q.limit, c, q.GetQueryInput())
}

func (q *QueryBuilder) GetQueryReader(c *dynamodb.Client) *QueryReader {
	return NewQueryReader(q.limit, c, q.GetQueryInput())
}

func (q *QueryBuilder) ExecGet(ctx context.Context, c *dynamodb.Client) (dynamodb.GetItemOutput, error) {
	out, err := c.GetItem(ctx, q.GetGetInput())
	if err != nil {
		return dynamodb.GetItemOutput{}, err
	}
	return *out, nil
}

func (q *QueryBuilder) ExecQuery(ctx context.Context, c *dynamodb.Client) (dynamodb.QueryOutput, error) {
	out, err := c.Query(ctx, q.GetQueryInput())
	if err != nil {
		return dynamodb.QueryOutput{}, err
	}
	return *out, nil
}
