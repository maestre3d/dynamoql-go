package transaction

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoDBStatement statement for Amazon DynamoDB.
type DynamoDBStatement struct {
	Table                     string
	ConditionExpression       string
	UpdateExpression          string
	Key                       map[string]types.AttributeValue
	Item                      map[string]types.AttributeValue
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
}

// DynamoDBDriver Amazon DynamoDB Driver for transaction operations.
type DynamoDBDriver struct {
	c *dynamodb.Client
}

// RegisterDynamoDB sets a DynamoDBDriver into transaction's driver list using DynamoDBDriverKey as key.
func RegisterDynamoDB(c *dynamodb.Client) {
	RegisterDriver(DynamoDBDriverKey, &DynamoDBDriver{c: c})
}

var _ Driver = &DynamoDBDriver{}

func marshalDynamoStatement(stmt Statement) (types.TransactWriteItem, error) {
	dynamoStmt, ok := stmt.Operation.(DynamoDBStatement)
	if !ok {
		return types.TransactWriteItem{}, ErrInvalidOperationType
	}
	switch stmt.Kind {
	case ReadKind:
		return types.TransactWriteItem{
			ConditionCheck: &types.ConditionCheck{
				ConditionExpression:                 newSafeStringPtr(dynamoStmt.ConditionExpression),
				Key:                                 dynamoStmt.Key,
				TableName:                           newSafeStringPtr(dynamoStmt.Table),
				ExpressionAttributeNames:            dynamoStmt.ExpressionAttributeNames,
				ExpressionAttributeValues:           dynamoStmt.ExpressionAttributeValues,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
			},
		}, nil
	case UpsertKind, InsertKind:
		return types.TransactWriteItem{
			Put: &types.Put{
				Item:                                dynamoStmt.Item,
				TableName:                           newSafeStringPtr(dynamoStmt.Table),
				ConditionExpression:                 newSafeStringPtr(dynamoStmt.ConditionExpression),
				ExpressionAttributeNames:            dynamoStmt.ExpressionAttributeNames,
				ExpressionAttributeValues:           dynamoStmt.ExpressionAttributeValues,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
			},
		}, nil
	case UpdateKind:
		return types.TransactWriteItem{
			Update: &types.Update{
				Key:                                 dynamoStmt.Key,
				TableName:                           newSafeStringPtr(dynamoStmt.Table),
				UpdateExpression:                    newSafeStringPtr(dynamoStmt.UpdateExpression),
				ConditionExpression:                 newSafeStringPtr(dynamoStmt.ConditionExpression),
				ExpressionAttributeNames:            dynamoStmt.ExpressionAttributeNames,
				ExpressionAttributeValues:           dynamoStmt.ExpressionAttributeValues,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
			},
		}, nil
	case DeleteKind:
		return types.TransactWriteItem{
			Delete: &types.Delete{
				Key:                                 dynamoStmt.Key,
				TableName:                           newSafeStringPtr(dynamoStmt.Table),
				ConditionExpression:                 newSafeStringPtr(dynamoStmt.ConditionExpression),
				ExpressionAttributeNames:            dynamoStmt.ExpressionAttributeNames,
				ExpressionAttributeValues:           dynamoStmt.ExpressionAttributeValues,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
			},
		}, nil
	default:
		return types.TransactWriteItem{}, ErrUnknownOperationKind
	}
}

func marshalDynamoStatements(stmts []Statement) ([]types.TransactWriteItem, error) {
	buf := make([]types.TransactWriteItem, 0, len(stmts))
	for _, stmt := range stmts {
		item, err := marshalDynamoStatement(stmt)
		if err != nil {
			return nil, err
		}
		buf = append(buf, item)
	}
	return buf, nil
}

func (d *DynamoDBDriver) Exec(ctx context.Context, stmts []Statement) error {
	id, err := GetID(ctx)
	if err != nil {
		return err
	}
	idStr := strconv.Itoa(id)
	items, err := marshalDynamoStatements(stmts)
	if err != nil {
		return err
	}
	_, err = d.c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems:               items,
		ClientRequestToken:          &idStr,
		ReturnConsumedCapacity:      "",
		ReturnItemCollectionMetrics: "",
	})
	return err
}
