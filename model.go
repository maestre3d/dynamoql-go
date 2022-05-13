package dynamodb_go

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Model struct {
	TableName string
}

func (m Model) Save(ctx context.Context, c *dynamodb.Client) error {
	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{
		Item:                        m.MarshalDynamoDB(),
		TableName:                   &m.TableName,
		ConditionExpression:         nil,
		ConditionalOperator:         "",
		Expected:                    nil,
		ExpressionAttributeNames:    nil,
		ExpressionAttributeValues:   nil,
		ReturnConsumedCapacity:      "",
		ReturnItemCollectionMetrics: "",
		ReturnValues:                "",
	})
	return err
}

func (m Model) Select(fields ...string) *QueryBuilder {
	return NewQueryBuilder(NewQueryBuilderArgs{
		Table: m.TableName,
	}).Select(fields)
}

func (m Model) MarshalDynamoDB() map[string]types.AttributeValue {
	return nil
}

func (m Model) UnmarshalDynamoDB(v map[string]types.AttributeValue) {
}
