package repository

import (
	"context"
	"example/global"
	"example/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func SaveClassroom(ctx context.Context, c *dynamodb.Client, classroom model.Classroom) error {
	marshal, err := classroom.MarshalDynamoDB()
	if err != nil {
		return err
	}
	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      marshal,
		TableName: aws.String(global.TableName),
	})
	return err
}
