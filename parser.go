package dynamodb_go

import (
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ParseString converts the given Amazon DynamoDB attribute to a primitive string.
func ParseString(v types.AttributeValue) string {
	data, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return data.Value
}

func ParseInt(v types.AttributeValue) int {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, _ := strconv.Atoi(data.Value)
	return res
}

func ParseFloat64(v types.AttributeValue) float64 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, _ := strconv.ParseFloat(data.Value, 64)
	return res
}

func ParseStringSet(v types.AttributeValue) []string {
	data, ok := v.(*types.AttributeValueMemberSS)
	if !ok {
		return nil
	}
	return data.Value
}

func ParseBool(v types.AttributeValue) bool {
	data, ok := v.(*types.AttributeValueMemberBOOL)
	if !ok {
		return false
	}
	return data.Value
}
