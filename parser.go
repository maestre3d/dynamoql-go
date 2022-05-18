package dynamoql

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const defaultTimeFormat = time.RFC3339

func FormatAttribute(v interface{}) types.AttributeValue {
	switch v.(type) {
	case string:
		val := v.(string)
		return &types.AttributeValueMemberS{Value: val}
	case []string:
		val := v.([]string)
		return &types.AttributeValueMemberSS{Value: val}
	case float64:
		val := v.(float64)
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(val, 'f', -1, 64)}
	case []float64:
		val := v.([]float64)
		buf := make([]string, 0, len(val))
		for i := range val {
			buf = append(buf, strconv.FormatFloat(val[i], 'f', -1, 64))
		}
		return &types.AttributeValueMemberNS{Value: buf}
	case int:
		val := v.(int)
		return &types.AttributeValueMemberN{Value: strconv.Itoa(val)}
	case []int:
		val := v.([]int)
		buf := make([]string, 0, len(val))
		for i := range val {
			buf = append(buf, strconv.Itoa(val[i]))
		}
		return &types.AttributeValueMemberNS{Value: buf}
	case uint:
		val := v.(int)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(val), 10)}
	case []byte:
		val := v.([]byte)
		return &types.AttributeValueMemberB{Value: val}
	case [][]byte:
		val := v.([][]byte)
		return &types.AttributeValueMemberBS{Value: val}
	case bool:
		val := v.(bool)
		return &types.AttributeValueMemberBOOL{Value: val}
	case time.Time:
		val := v.(time.Time)
		var a [64]byte
		var b = a[:0]
		b = val.AppendFormat(b, defaultTimeFormat)
		return &types.AttributeValueMemberS{Value: string(b)}
	}
	return nil
}

// ParseString converts the given Amazon DynamoDB attribute into string.
func ParseString(v types.AttributeValue) string {
	data, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return data.Value
}

// ParseTime converts the given Amazon DynamoDB attribute into time.Time.
func ParseTime(v types.AttributeValue) time.Time {
	data, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return time.Time{}
	}
	t, err := time.Parse(defaultTimeFormat, data.Value)
	if err != nil {
		panic(err)
	}
	return t
}

func ParseInt(v types.AttributeValue) int {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.Atoi(data.Value)
	if err != nil {
		panic(err)
	}
	return res
}

func ParseFloat64(v types.AttributeValue) float64 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseFloat(data.Value, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func ParseStringSet(v types.AttributeValue) []string {
	data, ok := v.(*types.AttributeValueMemberSS)
	if !ok {
		return nil
	}
	return data.Value
}

func ParseBinary(v types.AttributeValue) []byte {
	data, ok := v.(*types.AttributeValueMemberB)
	if !ok {
		return nil
	}
	return data.Value
}

func ParseBinarySet(v types.AttributeValue) [][]byte {
	data, ok := v.(*types.AttributeValueMemberBS)
	if !ok {
		return nil
	}
	return data.Value
}

func ParseFloat64Set(v types.AttributeValue) []float64 {
	data, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return nil
	}
	buf := make([]float64, 0, len(data.Value))
	for i := range data.Value {
		val, err := strconv.ParseFloat(data.Value[i], 64)
		if err != nil {
			panic(err)
			return nil
		}
		buf = append(buf, val)
	}
	return buf
}

func ParseIntSet(v types.AttributeValue) []int {
	data, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return nil
	}
	buf := make([]int, 0, len(data.Value))
	for i := range data.Value {
		val, err := strconv.Atoi(data.Value[i])
		if err != nil {
			panic(err)
			return nil
		}
		buf = append(buf, val)
	}
	return buf
}

func ParseBool(v types.AttributeValue) bool {
	data, ok := v.(*types.AttributeValueMemberBOOL)
	if !ok {
		return false
	}
	return data.Value
}
