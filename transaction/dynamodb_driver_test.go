//go:build integration

package transaction_test

import (
	"context"
	"testing"

	"github.com/maestre3d/dynamoql/transaction"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	dynamoDriverTable = "DriverTest"
)

type dynamoDriverTestSuite struct {
	suite.Suite

	client *dynamodb.Client
}

func TestDynamoDBDriver(t *testing.T) {
	suite.Run(t, &dynamoDriverTestSuite{})
}

func (s *dynamoDriverTestSuite) createTable() {
	_, err := s.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("partition_key"),
				AttributeType: "S",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("partition_key"),
				KeyType:       "HASH",
			},
		},
		TableName: aws.String(dynamoDriverTable),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(s.T(), err)
}

func (s *dynamoDriverTestSuite) seed() {
	_, err := s.client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"partition_key": &types.AttributeValueMemberS{
				Value: "123",
			},
		},
		TableName: aws.String(dynamoDriverTable),
	})
	require.NoError(s.T(), err)
}

func (s *dynamoDriverTestSuite) dropTable() {
	_, err := s.client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(dynamoDriverTable),
	})
	require.NoError(s.T(), err)
}

func (s *dynamoDriverTestSuite) SetupSuite() {
	s.client = newDynamoClient()
	transaction.RegisterDynamoDB(s.client)
	s.createTable()
	s.seed()
}

func (s *dynamoDriverTestSuite) SetupTest() {}

func (s *dynamoDriverTestSuite) TearDownSuite() {
	s.dropTable()
}

func (s *dynamoDriverTestSuite) TearDownTest() {}

func (s *dynamoDriverTestSuite) TestDynamoDBDriver_Exec() {
	ctx := transaction.NewContextWithDriver(context.TODO(), transaction.DynamoDBDriverKey)
	err := transaction.Append(ctx, transaction.Statement{
		Kind: transaction.InsertKind,
		Operation: transaction.DynamoDBStatement{
			Table: dynamoDriverTable,
			Item: map[string]types.AttributeValue{
				"partition_key": &types.AttributeValueMemberS{
					Value: "456",
				},
			},
		},
	}, transaction.Statement{
		Kind: transaction.DeleteKind,
		Operation: transaction.DynamoDBStatement{
			Table: dynamoDriverTable,
			Key: map[string]types.AttributeValue{
				"partition_key": &types.AttributeValueMemberS{
					Value: "123",
				},
			},
		},
	})
	require.NoError(s.T(), err)
	err = transaction.Exec(ctx)
	require.NoError(s.T(), err)
}
