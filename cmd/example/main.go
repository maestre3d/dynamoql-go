package main

import (
	"context"
	"encoding/json"
	"example/global"
	"example/model"
	"example/repository"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	awsLocalPartitionId = "aws"
	awsLocalHost        = "http://localhost:8000"
	awsLocalRegion      = "us-east-1"
)

func main() {
	ctx := context.Background()
	client := newDynamoClient()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("partition_key"),
				AttributeType: "S",
			},
			{
				AttributeName: aws.String("sort_key"),
				AttributeType: "S",
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("partition_key"),
				KeyType:       "HASH",
			},
			{
				AttributeName: aws.String("sort_key"),
				KeyType:       "RANGE",
			},
		},
		TableName:   aws.String(global.TableName),
		BillingMode: "",
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(global.GsiName),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("sort_key"),
						KeyType:       "HASH",
					},
					{
						AttributeName: aws.String("partition_key"),
						KeyType:       "RANGE",
					},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
		LocalSecondaryIndexes: nil,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		SSESpecification:    nil,
		StreamSpecification: nil,
		TableClass:          "",
		Tags:                nil,
	})
	if err != nil && !strings.Contains(err.Error(), "ResourceInUseException") {
		panic(err)
	}

	_ = repository.SaveClassroom(ctx, client, model.Classroom{
		FacilityID:  "123",
		ClassroomID: "abc",
		DisplayName: "Computing",
	})
	_ = repository.SaveClassroom(ctx, client, model.Classroom{
		FacilityID:  "456",
		ClassroomID: "xyz",
		DisplayName: "Lab",
	})
	_ = repository.SaveStudent(ctx, client, model.Student{
		StudentID:   "123-abc",
		DisplayName: "Bruno Gramer",
		Picture:     "https://scholar.static.neutrinocorp.org/students/123-abc.jpeg",
	})
	_ = repository.AssignStudentToClassroom(ctx, client, model.StudentClassroom{
		ClassroomID: "abc",
		StudentID:   "123-abc",
		FacilityID:  "123",
		AddedAt:     time.Now().UTC(),
	})
	_ = repository.AssignStudentToClassroom(ctx, client, model.StudentClassroom{
		ClassroomID: "xyz",
		StudentID:   "123-abc",
		FacilityID:  "456",
		AddedAt:     time.Now().UTC(),
	})
	_ = repository.SaveInvoice(ctx, client, model.Invoice{
		StudentID: "123-abc",
		InvoiceID: "1420",
		AddedAt:   time.Now().UTC(),
		Balance:   28458338.00,
		DueDate:   "10/31/25",
		Status:    "Cancelled",
	})
	_ = repository.SaveInvoice(ctx, client, model.Invoice{
		StudentID: "123-abc",
		InvoiceID: "1425",
		AddedAt:   time.Now().UTC(),
		Balance:   8458338.00,
		DueDate:   "12/31/25",
		Status:    "Active",
	})

	student := model.Student{
		StudentID: "123-abc",
	}
	if err = repository.GetStudentHydrate(ctx, client, &student); err != nil {
		panic(err)
	}

	studentJSON, _ := json.Marshal(student)
	log.Printf("%s", string(studentJSON))
}

func newDynamoClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	return dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.Credentials = credentials.NewStaticCredentialsProvider("LOCAL", "SECRET", "TOKEN")
		options.EndpointResolver = newDynamoLocalResolver()
	})
}

func newDynamoLocalResolver() dynamodb.EndpointResolverFunc {
	return func(region string, options dynamodb.EndpointResolverOptions) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   awsLocalPartitionId,
			URL:           awsLocalHost,
			SigningRegion: awsLocalRegion,
		}, nil
	}
}
