package main

import (
	"context"
	"example/model"
	"example/repository"
	"log"
	"time"

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
	if err := repository.GetStudentHydrate(ctx, client, &student); err != nil {
		panic(err)
	}

	log.Printf("%+v", student)
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
