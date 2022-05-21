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
	BookID      string
	DisplayName string
	ISBN        string
	Price       float32
	Stock       int
	PublishDate time.Time
	Picture     string
}

var _ dynamoql.Schema = &Book{}

func (b Book) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(b.BookID),
		"display_name":  dynamoql.FormatAttribute(b.DisplayName),
		"isbn":          dynamoql.FormatAttribute(b.ISBN),
		"publish_date":  dynamoql.FormatAttribute(b.PublishDate),
		"price":         dynamoql.FormatAttribute(b.Price),
		"stock":         dynamoql.FormatAttribute(b.Stock),
		"picture":       dynamoql.FormatAttribute(b.Picture),
	}, nil
}

func (b *Book) UnmarshalDynamoDB(v map[string]types.AttributeValue) error {
	b.BookID = dynamoql.MustParseString(v["partition_key"])
	b.DisplayName = dynamoql.MustParseString(v["display_name"])
	b.ISBN = dynamoql.MustParseString(v["isbn"])
	b.Price = dynamoql.MustParseFloat32(v["price"])
	b.Stock = dynamoql.MustParseInt(v["stock"])
	b.PublishDate = dynamoql.MustParseTime(v["publish_date"])
	b.Picture = dynamoql.MustParseString(v["picture"])
	return nil
}

func main() {
	ctx := context.Background()
	client := newDynamoClient()

	// uses composite key, hence two conditions required
	res, err := dynamoql.
		Select().
		From("Graph").
		Where(dynamoql.Condition{
			Operator: dynamoql.Equals,
			Field:    "partition_key",
			Value:    "book#456",
		}, dynamoql.Condition{
			Operator: dynamoql.Equals,
			Field:    "sort_key",
			Value:    "book#456",
		}).
		Metrics(types.ReturnConsumedCapacityTotal).
		ExecGet(ctx, client)
	if err != nil {
		panic(err)
	}
	book := Book{}
	_ = book.UnmarshalDynamoDB(res.Item)
	log.Printf("%+v", book)

	// Query
	out, err := dynamoql.
		Select().
		From("Graph").
		Where(dynamoql.Condition{
			Operator: dynamoql.Equals,
			Field:    "sort_key",
			Value:    "author#abc",
			IsKey:    true,
		}, dynamoql.Condition{
			Operator: dynamoql.BeginsWith,
			Field:    "partition_key",
			Value:    "book#",
			IsKey:    true,
		}).
		Or().
		Limit(1).
		Index("GsiOverload").
		OrderBy(dynamoql.Descend).
		Metrics(types.ReturnConsumedCapacityTotal).
		ExecQuery(ctx, client)
	if err != nil {
		panic(err)
	}
	log.Print(out.NextPageToken.String())
	buf := make([]Book, 0, len(out.Items))
	for _, item := range out.Items {
		b := Book{}
		if err = b.UnmarshalDynamoDB(item); err == nil {
			buf = append(buf, b)
		}
	}
	log.Printf("%+v", buf)
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
