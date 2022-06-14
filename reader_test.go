//go:build integration

package dynamoql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/maestre3d/dynamoql"
)

type queryReaderTestSuite struct {
	suite.Suite

	client *dynamodb.Client
}

func TestNewQueryReader(t *testing.T) {
	suite.Run(t, &queryReaderTestSuite{})
}

func (s *queryReaderTestSuite) SetupSuite() {
	s.client = newDynamoClient()
}

func (s *queryReaderTestSuite) SetupTest() {}

func (s *queryReaderTestSuite) TearDownTest() {}

func (s *queryReaderTestSuite) TearDownSuite() {}

func (s *queryReaderTestSuite) TestQueryReader_GetItem() {
	tests := []struct {
		name     string
		query    dynamodb.QueryInput
		pageSize int32
		expItems int
		wantErr  bool
	}{
		{
			name:     "Empty query",
			query:    dynamodb.QueryInput{},
			pageSize: 0,
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
			expItems: 0,
			wantErr:  true, // AWS SDK malformed query error
		},
		{
			name: "Valid empty result",
			query: dynamoql.NewQueryInput(dynamoql.Select().From("InvoiceAndBills").Where(dynamoql.Condition{
				IsKey:    true,
				Operator: dynamoql.Equals,
				Field:    "PK",
				Value:    dynamoql.NewCompositeKey("I", "abc"),
			}, dynamoql.Condition{
				IsKey:    true,
				Operator: dynamoql.BeginsWith,
				Field:    "SK",
				Value:    dynamoql.NewCompositeKey("B", ""),
			})),
			pageSize: 100,
			expItems: 0,
			wantErr:  true, // Reader has reached end of file (ErrReaderEOF), no items found
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
			pageSize: 100,
			expItems: 4,
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			r := dynamoql.NewQueryReader(tt.pageSize, s.client, tt.query)

			ctx := context.Background()
			bills := make([]Bill, 0, tt.expItems)
			for r.Next() {
				item, err := r.GetItem(ctx)
				require.Equal(t, tt.wantErr, err != nil)
				if err != nil {
					break
				}

				bill := Bill{}
				err = bill.UnmarshalDynamoDB(item)
				require.Nil(t, err)

				bills = append(bills, bill)
				if r.Count() >= tt.expItems {
					break
				}
			}
			assert.Equal(t, r.Count(), tt.expItems)
			assert.Len(t, bills, tt.expItems)
		})
	}
}
