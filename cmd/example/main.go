package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

const (
	awsLocalPartitionId = "aws"
	awsLocalHost        = "http://localhost:8000"
	awsLocalRegion      = "us-east-1"
)

type Author struct {
	AuthorID    string
	DisplayName string
}

type Book struct {
	Author      string
	BookID      string
	DisplayName string
	ISBN        string
	Price       float64
	Stock       int
	PublishDate time.Time
}

var _ dynamoql.Marshaler = Book{}
var _ dynamoql.Unmarshaler = &Book{}

func (b Book) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"book_id":      dynamoql.FormatAttribute(b.BookID),
		"author_id":    dynamoql.FormatAttribute(b.Author),
		"display_name": dynamoql.FormatAttribute(b.DisplayName),
		"isbn":         dynamoql.FormatAttribute(b.ISBN),
		"publish_date": dynamoql.FormatAttribute(b.PublishDate),
		"price":        dynamoql.FormatAttribute(b.Price),
		"stock":        dynamoql.FormatAttribute(b.Stock),
	}, nil
}

func (b *Book) UnmarshalDynamoDB(v map[string]types.AttributeValue) error {
	b.Author = dynamoql.ParseString(v["author_id"])
	b.BookID = dynamoql.ParseString(v["book_id"])
	b.DisplayName = dynamoql.ParseString(v["display_name"])
	b.ISBN = dynamoql.ParseString(v["isbn"])
	b.Price = dynamoql.ParseFloat64(v["price"])
	b.Stock = dynamoql.ParseInt(v["stock"])
	b.PublishDate = dynamoql.ParseTime(v["publish_date"])
	return nil
}

func main() {
	ctx := context.Background()
	client := newDynamoClient()

	// uses composite key, hence two conditions required
	res, err := dynamoql.
		Select().
		From("Books").
		Where(dynamoql.Condition{
			Operator: dynamoql.Equals,
			Key:      "book_id",
			Value:    "1",
		}, dynamoql.Condition{
			Operator: dynamoql.Equals,
			Key:      "author_id",
			Value:    "foobar-123",
		}).
		WithMetrics(types.ReturnConsumedCapacityTotal).
		Get(ctx, client)
	if err != nil {
		panic(err)
	}
	book := Book{}
	_ = book.UnmarshalDynamoDB(res.Item)

	// Query
	out, err := dynamoql.
		Select("book_id", "isbn", "publish_date").
		From("Books").
		Where(dynamoql.Condition{
			Operator: dynamoql.GreaterOrEqualThan,
			Key:      "price",
			Value:    99.99,
		}, dynamoql.Condition{
			Operator: dynamoql.Equals,
			Key:      "stock",
			Value:    0,
			Negate:   true,
		}).
		And().
		Limit(8).
		OrderBy(dynamoql.Descend).
		WithMetrics(types.ReturnConsumedCapacityTotal).
		WithStrongConsistency().
		PageToken(dynamoql.NewPageTokenString("U3xhdXRob3JfaWQ9MTIzLWFiYw==")).
		Query(ctx, client)
	if err != nil {
		panic(err)
	}
	log.Print(out.Items)
	log.Print(out.NextPageToken.String())
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
