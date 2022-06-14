package model

import (
	"example/global"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

type Invoice struct {
	StudentID string    `json:"student_id"`
	InvoiceID string    `json:"invoice_id"`
	AddedAt   time.Time `json:"added_at"`
	Balance   float64   `json:"balance"`
	DueDate   string    `json:"due_date"`
	Status    string    `json:"status"`

	Student *Student `json:"student,omitempty"`
}

var _ dynamoql.NodeSchema = &Invoice{}

func (i Invoice) GetName() string {
	return "Invoice"
}

func (i Invoice) GetKeys() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, i.StudentID)),
		"sort_key":      dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.InvoiceKeyName, i.InvoiceID)),
	}
}

func (i Invoice) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"partition_key":             dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, i.StudentID)),
		"sort_key":                  dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.InvoiceKeyName, i.InvoiceID)),
		"invoice_balance":           dynamoql.FormatAttribute(i.Balance),
		"invoice_due_date":          dynamoql.FormatAttribute(i.DueDate),
		"invoice_status":            dynamoql.FormatAttribute(i.Status),
		"added_at":                  dynamoql.FormatAttribute(i.AddedAt),
		dynamoql.DefaultSchemaField: dynamoql.FormatAttribute(i.GetName()),
	}, nil
}

func (i *Invoice) UnmarshalDynamoDB(m map[string]types.AttributeValue) error {
	i.StudentID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["partition_key"]))
	i.InvoiceID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["sort_key"]))
	i.Balance = dynamoql.MustParseFloat64(m["invoice_balance"])
	i.DueDate = dynamoql.MustParseString(m["invoice_due_date"])
	i.Status = dynamoql.MustParseString(m["invoice_status"])
	i.AddedAt = dynamoql.MustParseTime(m["added_at"])
	return nil
}
