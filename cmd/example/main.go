package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/config"
	dynamodb2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodb "github.com/maestre3d/dynamodb-go"
)

type Book struct {
	dynamodb.Model
	Author      string `dynamodb:"author,partition_key"`
	BookID      string `dynamodb:"book_id,sort_key"`
	DisplayName string
	ISBN        string
	PublishDate time.Time
}

func (b Book) ToDynamo() map[string]types.AttributeValue {
	log.Print("custom book marshal")
	return nil
}

func main() {
	ctx := context.Background()
	client := newDynamoClient()

	book := Book{
		Model: dynamodb.Model{
			TableName: "books",
		},
	}
	book.ToDynamo()
	_ = book.Save(ctx, client)

	out, token, err := book.
		Select("book_id", "isbn", "publish_date").
		Where("book_id", "2", dynamodb.Equals, false).
		Limit(8).
		OrderBy(dynamodb.Descend).
		PageToken(dynamodb.PageToken{"book_id": &types.AttributeValueMemberS{Value: "1"}}).
		Exec(ctx, client)
	if err != nil {
		panic(err)
	}
	log.Print(token)
	log.Print(out)
}

func newDynamoClient() *dynamodb2.Client {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	return dynamodb2.NewFromConfig(cfg)
}
