package model

import (
	"errors"
	"example/global"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
)

type Student struct {
	StudentID   string
	DisplayName string
	Picture     string

	Classrooms []Classroom // populated manually
	Invoices   []Invoice
}

var _ dynamoql.NodeSchema = &Student{}

func (b Student) GetName() string {
	return "Student"
}

func (b Student) GetKeys() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
		"sort_key":      dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
	}
}

func (b Student) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"partition_key":             dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
		"sort_key":                  dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
		"display_name":              dynamoql.FormatAttribute(b.DisplayName),
		"picture":                   dynamoql.FormatAttribute(b.Picture),
		dynamoql.DefaultSchemaField: dynamoql.FormatAttribute(b.GetName()), // used to identify model using Batch API
	}, nil
}

func (b *Student) UnmarshalDynamoDB(v map[string]types.AttributeValue) error {
	if schema := dynamoql.MustParseString(v[dynamoql.DefaultSchemaField]); schema != b.GetName() {
		return errors.New("invalid schema")
	}
	b.StudentID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(v["partition_key"]))
	b.DisplayName = dynamoql.MustParseString(v["display_name"])
	b.Picture = dynamoql.MustParseString(v["picture"])
	return nil
}

// GSI Overloading will do the heavy-lifting for us as it will project most of the data automatically.
//
// Therefore, if we want to fetch all student's classrooms, we use the overloaded GSI.
// Moreover, if we want to fetch all classroom's students, we use default table (no GSI).

// StudentClassroom Many to many Student - Classroom.
type StudentClassroom struct {
	ClassroomID string
	StudentID   string
	FacilityID  string // required to hydrate a posteriori
	AddedAt     time.Time
}

var _ dynamoql.EdgeSchema = &StudentClassroom{}

func (b StudentClassroom) GetLeftKeys() map[string]types.AttributeValue {
	return Classroom{
		FacilityID:  b.FacilityID,
		ClassroomID: b.ClassroomID,
	}.GetKeys()
}

func (b StudentClassroom) GetRightKeys() map[string]types.AttributeValue {
	return Student{
		StudentID: b.FacilityID,
	}.GetKeys()
}

func (b StudentClassroom) GetKeys() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.ClassroomKeyName, b.ClassroomID)),
		"sort_key":      dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
	}
}

func (b StudentClassroom) MarshalDynamoDB() (map[string]types.AttributeValue, error) {
	return map[string]types.AttributeValue{
		"partition_key": dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.ClassroomKeyName, b.ClassroomID)),
		"sort_key":      dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.StudentKeyName, b.StudentID)),
		"facility_id":   dynamoql.FormatAttribute(dynamoql.NewCompositeKey(global.FacilityKeyName, b.FacilityID)),
		"added_time":    dynamoql.FormatAttribute(b.AddedAt),
	}, nil
}

func (b *StudentClassroom) UnmarshalDynamoDB(v map[string]types.AttributeValue) error {
	b.ClassroomID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(v["partition_key"]))
	b.StudentID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(v["sort_key"]))
	b.FacilityID = dynamoql.ParseCompositeKey(dynamoql.MustParseString(v["facility_id"]))
	b.AddedAt = dynamoql.MustParseTime(v["added_time"])
	return nil
}
