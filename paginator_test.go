//go:build integration

package dynamoql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/maestre3d/dynamoql"
	"github.com/stretchr/testify/suite"
)

type queryPaginatorTestSuite struct {
	suite.Suite

	client *dynamodb.Client
}

func TestNewQueryPaginator(t *testing.T) {
	suite.Run(t, &queryPaginatorTestSuite{})
}

func (s *queryPaginatorTestSuite) SetupSuite() {
	s.client = newDynamoClient()
}

func (s *queryPaginatorTestSuite) SetupTest() {}

func (s *queryPaginatorTestSuite) TearDownTest() {}

func (s *queryPaginatorTestSuite) TearDownSuite() {}

func (s *queryPaginatorTestSuite) TestQueryPaginator_GetPage() {
	tests := []struct {
		name         string
		query        dynamodb.QueryInput
		pageSize     int32
		scannedPages uint32
		expItems     int32
		wantErr      bool
	}{
		{
			name:     "Empty query",
			query:    dynamodb.QueryInput{}, // missing table
			pageSize: 0,
			wantErr:  true,
		},
		{
			name:     "Empty query with table name",
			query:    dynamoql.NewQueryInput(dynamoql.Select().From("InvoiceAndBills")),
			pageSize: 0,
			wantErr:  true,
		},
		{
			name: "Invalid query",
			query: dynamoql.NewQueryInput(dynamoql.Select().From("InvoiceAndBills").Where(dynamoql.Condition{
				IsKey:    true,
				Operator: dynamoql.Equals,
				Field:    "PK",
				Value:    dynamoql.NewCompositeKey("I", "1191"),
			}, dynamoql.Condition{
				IsKey:    false,
				Operator: dynamoql.Equals,
				Field:    "SK",
				Value:    dynamoql.NewCompositeKey("B", ""),
			})),
			pageSize: 0,
			wantErr:  true,
		},
		{
			name: "Valid",
			query: dynamoql.NewQueryInput(dynamoql.Select().From("InvoiceAndBills").Where(dynamoql.Condition{
				IsKey:    true,
				Operator: dynamoql.Equals,
				Field:    "PK",
				Value:    dynamoql.NewCompositeKey("I", "1191"),
			}, dynamoql.Condition{
				IsKey:    true,
				Operator: dynamoql.BeginsWith,
				Field:    "SK",
				Value:    dynamoql.NewCompositeKey("B", ""),
			})),
			pageSize:     100,
			scannedPages: 1,
			expItems:     4,
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			p := dynamoql.NewQueryPaginator(tt.pageSize, s.client, tt.query)
			ctx := context.Background()
			itemBuf := dynamoql.NewItemBuffer(int(tt.expItems))
			for p.Next() {
				out, err := p.GetPage(ctx)
				require.Equal(t, tt.wantErr, err != nil)
				if err != nil {
					break
				}

				itemBuf.WriteItems(out.Items)
				if p.Count() >= tt.expItems {
					break
				}
			}
			assert.Nil(t, p.NextPageToken())
			assert.Equal(t, p.ScannedPages(), tt.scannedPages)
			assert.Equal(t, p.Count(), tt.expItems)
			assert.Equal(t, itemBuf.Len(), int(tt.expItems))
		})
	}
}
