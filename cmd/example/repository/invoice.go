package repository

import (
	"context"
	"example/global"
	"example/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func SaveInvoice(ctx context.Context, c *dynamodb.Client, m model.Invoice) error {
	marshal, err := m.MarshalDynamoDB()
	if err != nil {
		return err
	}
	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      marshal,
		TableName: aws.String(global.TableName),
	})
	return err
}
