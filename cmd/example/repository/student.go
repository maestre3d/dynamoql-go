package repository

import (
	"context"
	"example/global"
	"example/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

func GetStudent(ctx context.Context, c *dynamodb.Client, student *model.Student) error {
	out, err := dynamoql.Select().From(global.TableName).
		Where(dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.Equals,
			Field:    "partition_key",
			Value:    dynamoql.NewCompositeKey(global.StudentKeyName, student.StudentID),
		}, dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.Equals,
			Field:    "sort_key",
			Value:    dynamoql.NewCompositeKey(global.StudentKeyName, student.StudentID),
		}).ExecGet(ctx, c)
	if err != nil {
		return err
	}
	return student.UnmarshalDynamoDB(out.Item)
}

type Iterator func(map[string]types.AttributeValue) map[string]types.AttributeValue

func hydrate(query []map[string]types.AttributeValue, iterator Iterator) {
	for _, item := range query {
		keys := iterator(item)
		if keys == nil {
			continue
		}
	}
}

func GetStudentHydrate(ctx context.Context, c *dynamodb.Client, student *model.Student) error {
	// 1. Get relationships (Get all classrooms assigned to a student).
	//
	// Note: Base table uses classroom_id as PK and student_id as SK.
	// Hence, the usage of the overloaded GSI is required.
	// N-M relation
	outQuery, err := dynamoql.Select().From(global.TableName).
		Where(dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.Equals,
			Field:    "sort_key",
			Value:    dynamoql.NewCompositeKey(global.StudentKeyName, student.StudentID),
		}, dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.BeginsWith,
			Field:    "partition_key",
			Value:    dynamoql.NewCompositeKey(global.ClassroomKeyName, ""),
		}).
		Index(global.GsiName).
		Limit(100). // DO NOT hydrate many schemas as Batch and Query APIs have byte size limitations of response
		ExecQuery(ctx, c)
	if err != nil {
		return err
	}
	// 1-N relation
	outQueryInv, err := dynamoql.Select().From(global.TableName).
		Where(dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.Equals,
			Field:    "partition_key",
			Value:    dynamoql.NewCompositeKey(global.StudentKeyName, student.StudentID),
		}, dynamoql.Condition{
			IsKey:    true,
			Operator: dynamoql.BeginsWith,
			Field:    "sort_key",
			Value:    dynamoql.NewCompositeKey(global.InvoiceKeyName, ""),
		}).
		Limit(100). // DO NOT hydrate many schemas as Batch and Query APIs have byte size limitations of response
		ExecQuery(ctx, c)
	if err != nil {
		return err
	}
	for _, item := range outQueryInv.Items {
		marshal := model.Invoice{}
		err = marshal.UnmarshalDynamoDB(item)
		if err != nil {
			continue
		}
		student.Invoices = append(student.Invoices, marshal)
	}

	// 2. Build key slice out of items to hydrate using Batch API
	bufKeys := make([]map[string]types.AttributeValue, 0, 1+outQuery.Count)
	bufKeys = append(bufKeys, student.GetKeys())

	for _, item := range outQuery.Items {
		marshal := model.StudentClassroom{}
		err = marshal.UnmarshalDynamoDB(item)
		if err != nil {
			continue
		}
		bufKeys = append(bufKeys, marshal.GetLeftKeys())
	}

	// 3. Perform the actual fetching
	out, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			global.TableName: {
				Keys:                     bufKeys,
				AttributesToGet:          nil,
				ConsistentRead:           nil,
				ExpressionAttributeNames: nil,
				ProjectionExpression:     nil,
			},
		},
		ReturnConsumedCapacity: "",
	})
	if err != nil {
		return err
	}

	// 4. Unmarshal data, requires a way to determine which model is going to be decoded.
	for _, item := range out.Responses[global.TableName] {
		if err = student.UnmarshalDynamoDB(item); err == nil {
			continue
		}
		classroom := model.Classroom{}
		err = classroom.UnmarshalDynamoDB(item)
		if err != nil {
			continue
		}
		student.Classrooms = append(student.Classrooms, classroom)
	}

	return nil
}

func SaveStudent(ctx context.Context, c *dynamodb.Client, student model.Student) error {
	stMarshal, err := student.MarshalDynamoDB()
	if err != nil {
		return err
	}
	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      stMarshal,
		TableName: aws.String(global.TableName),
	})
	return err
}

func AssignStudentToClassroom(ctx context.Context, c *dynamodb.Client, m model.StudentClassroom) error {
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
