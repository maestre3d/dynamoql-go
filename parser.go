package dynamoql

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const defaultTimeFormat = time.RFC3339

// FormatAttribute converts a Go primitive type into a DynamoDB type.
func FormatAttribute(v interface{}) types.AttributeValue {
	switch v.(type) {
	case string:
		val := v.(string)
		return &types.AttributeValueMemberS{Value: val}
	case []string:
		val := v.([]string)
		return &types.AttributeValueMemberSS{Value: val}
	case float32:
		val := v.(float32)
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(float64(val), 'f', -1, 32)}
	case float64:
		val := v.(float64)
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(val, 'f', -1, 64)}
	case []float32:
		val := v.([]float32)
		buf := make([]string, 0, len(val))
		for i := range val {
			buf = append(buf, strconv.FormatFloat(float64(val[i]), 'f', -1, 32))
		}
		return &types.AttributeValueMemberNS{Value: buf}
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
	case int8:
		val := v.(int8)
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(val))}
	case int16:
		val := v.(int16)
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(val))}
	case int32:
		val := v.(int32)
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(val))}
	case int64:
		val := v.(int64)
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(val))}
	case []int:
		val := v.([]int)
		buf := make([]string, 0, len(val))
		for i := range val {
			buf = append(buf, strconv.Itoa(val[i]))
		}
		return &types.AttributeValueMemberNS{Value: buf}
	case uint:
		val := v.(uint)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(val), 10)}
	case uint8:
		val := v.(uint8)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(val), 10)}
	case uint16:
		val := v.(uint16)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(val), 10)}
	case uint32:
		val := v.(uint32)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(val), 10)}
	case uint64:
		val := v.(uint64)
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(val, 10)}
	case []uint:
		val := v.([]uint)
		buf := make([]string, 0, len(val))
		for i := range val {
			buf = append(buf, strconv.FormatUint(uint64(val[i]), 10))
		}
		return &types.AttributeValueMemberNS{Value: buf}
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

// ParseFloat64 converts the given Amazon DynamoDB attribute into 64-bit floating point.
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

// ParseFloat32 converts the given Amazon DynamoDB attribute into 32-bit floating point.
func ParseFloat32(v types.AttributeValue) float32 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseFloat(data.Value, 32)
	if err != nil {
		panic(err)
	}
	return float32(res)
}

// ParseInt converts the given Amazon DynamoDB attribute into a signed integer.
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

// ParseInt8 converts the given Amazon DynamoDB attribute into an 8-bit signed integer.
func ParseInt8(v types.AttributeValue) int8 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseInt(data.Value, 10, 8)
	if err != nil {
		panic(err)
	}
	return int8(res)
}

// ParseInt16 converts the given Amazon DynamoDB attribute into an 16-bit signed integer.
func ParseInt16(v types.AttributeValue) int16 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseInt(data.Value, 10, 16)
	if err != nil {
		panic(err)
	}
	return int16(res)
}

// ParseInt32 converts the given Amazon DynamoDB attribute into an 32-bit signed integer.
func ParseInt32(v types.AttributeValue) int32 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseInt(data.Value, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(res)
}

// ParseInt64 converts the given Amazon DynamoDB attribute into an 64-bit signed integer.
func ParseInt64(v types.AttributeValue) int64 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseInt(data.Value, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

// ParseUint converts the given Amazon DynamoDB attribute into an unsigned integer.
func ParseUint(v types.AttributeValue) uint {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseUint(data.Value, 10, 0)
	if err != nil {
		panic(err)
	}
	return uint(res)
}

// ParseUint8 converts the given Amazon DynamoDB attribute into an 8-bit unsigned integer.
func ParseUint8(v types.AttributeValue) uint8 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseUint(data.Value, 10, 8)
	if err != nil {
		panic(err)
	}
	return uint8(res)
}

// ParseUint16 converts the given Amazon DynamoDB attribute into an 16-bit unsigned integer.
func ParseUint16(v types.AttributeValue) uint16 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseUint(data.Value, 10, 16)
	if err != nil {
		panic(err)
	}
	return uint16(res)
}

// ParseUint32 converts the given Amazon DynamoDB attribute into an 32-bit unsigned integer.
func ParseUint32(v types.AttributeValue) uint32 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseUint(data.Value, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(res)
}

// ParseUint64 converts the given Amazon DynamoDB attribute into an 64-bit unsigned integer.
func ParseUint64(v types.AttributeValue) uint64 {
	data, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	res, err := strconv.ParseUint(data.Value, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

// ParseStringSet converts the given Amazon DynamoDB attribute into a string set.
func ParseStringSet(v types.AttributeValue) []string {
	data, ok := v.(*types.AttributeValueMemberSS)
	if !ok {
		return nil
	}
	return data.Value
}

// ParseBinary converts the given Amazon DynamoDB attribute into a slice of bytes.
func ParseBinary(v types.AttributeValue) []byte {
	data, ok := v.(*types.AttributeValueMemberB)
	if !ok {
		return nil
	}
	return data.Value
}

// ParseBinarySet converts the given Amazon DynamoDB attribute into a binary slice (matrix byte).
func ParseBinarySet(v types.AttributeValue) [][]byte {
	data, ok := v.(*types.AttributeValueMemberBS)
	if !ok {
		return nil
	}
	return data.Value
}

// ParseFloat64Set converts the given Amazon DynamoDB attribute into a float64 set.
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

// ParseFloat32Set converts the given Amazon DynamoDB attribute into a float32 set.
func ParseFloat32Set(v types.AttributeValue) []float32 {
	data, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return nil
	}
	buf := make([]float32, 0, len(data.Value))
	for i := range data.Value {
		val, err := strconv.ParseFloat(data.Value[i], 32)
		if err != nil {
			panic(err)
			return nil
		}
		buf = append(buf, float32(val))
	}
	return buf
}

// ParseIntSet converts the given Amazon DynamoDB attribute into a signed integer set.
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

// ParseUintSet converts the given Amazon DynamoDB attribute into an unsigned integer set.
func ParseUintSet(v types.AttributeValue) []uint {
	data, ok := v.(*types.AttributeValueMemberNS)
	if !ok {
		return nil
	}
	buf := make([]uint, 0, len(data.Value))
	for i := range data.Value {
		val, err := strconv.Atoi(data.Value[i])
		if err != nil {
			panic(err)
			return nil
		}
		buf = append(buf, uint(val))
	}
	return buf
}

// ParseBool converts the given Amazon DynamoDB attribute into boolean.
func ParseBool(v types.AttributeValue) bool {
	data, ok := v.(*types.AttributeValueMemberBOOL)
	if !ok {
		return false
	}
	return data.Value
}
