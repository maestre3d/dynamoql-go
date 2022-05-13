package dynamodb_go

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const defaultLimit = 10

type condition struct {
	negate   bool
	operator Operator
	a, b     string
}

type QueryBuilder struct {
	negate   bool
	limit    int32
	operator Operator
	ordering Ordering
	table    string

	fields     []string
	conditions []condition

	pageToken PageToken
}

type NewQueryBuilderArgs struct {
	Table string
}

func NewQueryBuilder(args NewQueryBuilderArgs) *QueryBuilder {
	return &QueryBuilder{
		negate:     false,
		limit:      defaultLimit,
		pageToken:  nil,
		operator:   "",
		table:      args.Table,
		fields:     nil,
		conditions: nil,
	}
}

func (q *QueryBuilder) Select(fields []string) *QueryBuilder {
	q.fields = fields
	return q
}

func (q *QueryBuilder) Negate() *QueryBuilder {
	q.negate = true
	return q
}

func (q *QueryBuilder) Where(a, b string, operator Operator, negate bool) *QueryBuilder {
	q.conditions = append(q.conditions, condition{
		a:        a,
		b:        b,
		operator: operator,
		negate:   negate,
	})
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

func (q *QueryBuilder) get(ctx context.Context, c *dynamodb.Client) ([]map[string]types.AttributeValue, PageToken, error) {
	out, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		Key:                      nil,
		TableName:                nil,
		AttributesToGet:          q.fields,
		ConsistentRead:           nil,
		ExpressionAttributeNames: nil,
		ProjectionExpression:     nil,
		ReturnConsumedCapacity:   "NONE",
	})
	if err != nil {
		return nil, nil, err
	}
	return []map[string]types.AttributeValue{
		out.Item,
	}, nil, nil
}

func (q *QueryBuilder) query(ctx context.Context, c *dynamodb.Client) ([]map[string]types.AttributeValue, PageToken, error) {
	out, err := c.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &q.table,
		AttributesToGet:           q.fields,
		ConditionalOperator:       "",
		ConsistentRead:            nil,
		ExclusiveStartKey:         q.pageToken,
		ExpressionAttributeNames:  nil,
		ExpressionAttributeValues: nil,
		FilterExpression:          nil,
		IndexName:                 nil,
		KeyConditionExpression:    nil,
		KeyConditions:             nil,
		Limit:                     &q.limit,
		ProjectionExpression:      nil,
		QueryFilter:               nil,
		ReturnConsumedCapacity:    "",
		ScanIndexForward:          nil,
		Select:                    "",
	})
	if err != nil {
		return nil, nil, err
	}
	return out.Items, out.LastEvaluatedKey, nil
}

func (q *QueryBuilder) Exec(ctx context.Context, c *dynamodb.Client) ([]map[string]types.AttributeValue, PageToken, error) {
	switch {
	case q.limit == 1 && q.pageToken == nil && len(q.conditions) == 1:
		return q.get(ctx, c)
	default:
		return q.query(ctx, c)
	}
}
