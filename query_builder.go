package dynamoql

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var DefaultQueryLimit int32 = 10

// Condition a comparison statement used by queries (QueryBuilder).
type Condition struct {
	// Negate sets an opposite value of the statement original output.
	Negate bool
	// IsKey sets the given statement as a Key comparison.
	IsKey bool
	// Operator statement comparison operator.
	Operator ConditionalOperator
	// SecondaryOperator an additional comparison operator.
	// Used by Size operator only.
	SecondaryOperator ConditionalOperator
	// Field attribute name used by the comparison statement.
	Field string
	// Value attribute actual value used by the comparison statement.
	Value interface{}
	// ExtraValues additional attribute values.
	// Used by Between and In operators only
	ExtraValues []interface{}
}

// QueryBuilder crafts an Amazon DynamoDB query statement ready to be used by Query, GetItem and Scan APIs.
//
// Moreover, it integrates with Paginator and Reader instances for seamless API usability.
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

// NewQueryBuilder builds a QueryBuilder instance.
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		negate:        false,
		limit:         DefaultQueryLimit,
		returnMetrics: types.ReturnConsumedCapacityNone,
		ordering:      Ascend,
	}
}

// Select builds a new QueryBuilder instance and sets attributes to be projected by the query.
func Select(projectedFields ...string) *QueryBuilder {
	return NewQueryBuilder().Select(projectedFields)
}

// Select sets attributes to be projected by the query.
func (q *QueryBuilder) Select(projectedFields []string) *QueryBuilder {
	if projectedFields != nil && len(projectedFields) > 0 {
		q.projectedFieldsExpression = aws.String(strings.Join(projectedFields, ","))
	}
	return q
}

// From sets the table to query from.
func (q *QueryBuilder) From(table string) *QueryBuilder {
	q.table = table
	return q
}

// Negate sets the returned output to be opposite.
func (q *QueryBuilder) Negate() *QueryBuilder {
	q.negate = true
	return q
}

// Where sets conditions statements.
func (q *QueryBuilder) Where(c ...Condition) *QueryBuilder {
	q.conditions = c
	return q
}

// And concatenates Condition(s) with an And operator.
func (q *QueryBuilder) And() *QueryBuilder {
	q.operator = And
	return q
}

// Or concatenates Condition(s) with an Or operator.
func (q *QueryBuilder) Or() *QueryBuilder {
	q.operator = Or
	return q
}

// Limit sets the length of the items to be returned by the query operation.
func (q *QueryBuilder) Limit(l int32) *QueryBuilder {
	q.limit = l
	return q
}

// PageToken sets the next page token for pagination query operations.
func (q *QueryBuilder) PageToken(t PageToken) *QueryBuilder {
	q.pageToken = t
	return q
}

// OrderBy sets the ordering of the item list to be returned by the query operation.
func (q *QueryBuilder) OrderBy(o Ordering) *QueryBuilder {
	q.ordering = o
	return q
}

// Index sets the Global Secondary Index to query from.
func (q *QueryBuilder) Index(i string) *QueryBuilder {
	if i != "" {
		q.index = &i
	}
	return q
}

// StrongConsistency sets the desired replication consistency between Amazon DynamoDB internal nodes while querying data.
// Default is eventual consistency, as every distributed platform is due CAP theorem.
func (q *QueryBuilder) StrongConsistency() *QueryBuilder {
	q.isConsistent = true
	return q
}

// Metrics sets the desired metric data from consumed capacity outputs.
func (q *QueryBuilder) Metrics(v types.ReturnConsumedCapacity) *QueryBuilder {
	q.returnMetrics = v
	return q
}

// DegreeOfParallelism sets the number of segments to be scanned in parallel.
//
// Note: Only available for Scan operations.
func (q *QueryBuilder) DegreeOfParallelism(d int32) *QueryBuilder {
	q.parallelDegree = d
	return q
}

// GetQueryPaginator builds a QueryPaginator using current QueryBuilder instance values.
func (q *QueryBuilder) GetQueryPaginator(c *dynamodb.Client) *QueryPaginator {
	return NewQueryPaginator(q.limit, c, NewQueryInput(q))
}

// GetQueryReader builds a *QueryReader using current QueryBuilder instance values.
func (q *QueryBuilder) GetQueryReader(c *dynamodb.Client) *QueryReader {
	return NewQueryReader(q.limit, c, NewQueryInput(q))
}

// ExecGet executes a GetItem API operation.
func (q *QueryBuilder) ExecGet(ctx context.Context, c *dynamodb.Client) (dynamodb.GetItemOutput, error) {
	in := NewGetInput(q)
	out, err := c.GetItem(ctx, &in)
	if err != nil {
		return dynamodb.GetItemOutput{}, err
	}
	return *out, nil
}

// ExecQuery executes a Query API operation.
func (q *QueryBuilder) ExecQuery(ctx context.Context, c *dynamodb.Client) (dynamodb.QueryOutput, error) {
	in := NewQueryInput(q)
	out, err := c.Query(ctx, &in)
	if err != nil {
		return dynamodb.QueryOutput{}, err
	}
	return *out, nil
}

// ExecScan executes a Scan API operation.
func (q *QueryBuilder) ExecScan(ctx context.Context, c *dynamodb.Client) (dynamodb.ScanOutput, error) {
	in := NewScanInput(q)
	out, err := c.Scan(ctx, &in)
	if err != nil {
		return dynamodb.ScanOutput{}, err
	}
	return *out, nil
}
