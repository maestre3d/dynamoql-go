package transaction

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoDBDriver Amazon DynamoDB Driver for transaction operations.
type DynamoDBDriver struct {
	c *dynamodb.Client
}

// RegisterDynamoDB sets a DynamoDBDriver into transaction's driver list using DynamoDBDriverName as key.
func RegisterDynamoDB(c *dynamodb.Client) {
	RegisterDriver(DynamoDBDriverKey, &DynamoDBDriver{c: c})
}

var _ Driver = &DynamoDBDriver{}

func (d *DynamoDBDriver) Commit(_ context.Context, _ []Statement) error {
	//TODO implement me
	panic("implement me")
}

func (d *DynamoDBDriver) Rollback(_ context.Context, _ []Statement) error {
	//TODO implement me
	panic("implement me")
}
