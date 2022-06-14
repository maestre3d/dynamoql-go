package model

import (
	"example/global"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

type Classroom struct {
	FacilityID  string
	ClassroomID string
	DisplayName string

	Students []Student // populated manually
}

var _ dynamoql.NodeSchema = &Classroom{}

func (c Classroom) GetName() string {
	return "Classroom"
}

func (c Classroom) GetKeys() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.FacilityKeyName, c.FacilityID)),
		"sort_key":      dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.ClassroomKeyName, c.ClassroomID)),
	}
}

func (c Classroom) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"partition_key":             dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.FacilityKeyName, c.FacilityID)),
		"sort_key":                  dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.ClassroomKeyName, c.ClassroomID)),
		"display_name":              dynamoql.FormatAttribute(c.DisplayName),
		dynamoql.DefaultSchemaField: dynamoql.FormatAttribute(c.GetName()),
	}, nil
}

func (c *Classroom) UnmarshalDynamoDB(m map[string]types.AttributeValue) error {
	c.FacilityID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["partition_key"]))
	c.ClassroomID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(m["sort_key"]))
	c.DisplayName = dynamoql.MustParseString(m["display_name"])
	return nil
}
