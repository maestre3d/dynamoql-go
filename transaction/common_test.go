package transaction_test

import (
	"context"

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

type Bill struct {
	InvoiceID string
	BillID    string
	Amount    string
	Balance   string
}

var _ dynamoql.NodeSchema = &Bill{}

func (i Bill) GetName() string {
	return "Bill"
}

func (i Bill) GetKeys() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"PK": dynamoql.FormatAttribute(dynamoql.NewCompositeKey("I", i.InvoiceID)),
		"SK": dynamoql.FormatAttribute(dynamoql.NewCompositeKey("B", i.BillID)),
	}
}

func (i Bill) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"PK":          dynamoql.FormatAttribute(dynamoql.NewCompositeKey("I", i.InvoiceID)),
		"SK":          dynamoql.FormatAttribute(dynamoql.NewCompositeKey("B", i.BillID)),
		"BillAmount":  dynamoql.FormatAttribute(i.Amount),
		"BillBalance": dynamoql.FormatAttribute(i.Balance),
	}, nil
}

func (i *Bill) UnmarshalDynamoDB(m map[string]types.AttributeValue) error {
	i.InvoiceID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["PK"]))
	i.BillID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["SK"]))
	i.Amount = dynamoql.MustParseString(m["BillAmount"])
	i.Balance = dynamoql.MustParseString(m["BillBalance"])
	return nil
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
