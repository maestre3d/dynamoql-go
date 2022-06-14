package dynamoql

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type QueryPaginator struct {
	client       *dynamodb.Client
	query        dynamodb.QueryInput
	lastEvalKey  PageToken
	scannedPages uint32
	itemCount    int32
}

func NewQueryPaginator(pageSize int32, c *dynamodb.Client, q dynamodb.QueryInput) *QueryPaginator {
	if pageSize > 0 {
		q.Limit = &pageSize
	}
	return &QueryPaginator{
		client: c,
		query:  q,
	}
}

func (p QueryPaginator) NextPageToken() PageToken {
	return p.lastEvalKey
}

func (p QueryPaginator) Next() bool {
	return p.lastEvalKey.String() != "" || p.scannedPages == 0
}

func (p QueryPaginator) ScannedPages() uint32 {
	return p.scannedPages
}

func (p QueryPaginator) Count() int32 {
	return p.itemCount
}

func (p *QueryPaginator) GetPage(ctx context.Context) (*dynamodb.QueryOutput, error) {
	p.query.ExclusiveStartKey = p.lastEvalKey
	out, err := p.client.Query(ctx, &p.query)
	if err != nil {
		return nil, err
	}
	p.lastEvalKey = out.LastEvaluatedKey
	p.scannedPages++
	p.itemCount += out.Count
	return out, err
}
