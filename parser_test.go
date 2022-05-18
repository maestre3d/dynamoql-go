package dynamoql_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
	"github.com/stretchr/testify/assert"
)

func TestFormatAttribute(t *testing.T) {
	tests := []struct {
		Name string
		In   interface{}
		Exp  types.AttributeValue
	}{
		{
			Name: "Empty",
			In:   nil,
			Exp:  nil,
		},
		{
			Name: "String",
			In:   "foo",
			Exp: &types.AttributeValueMemberS{
				Value: "foo",
			},
		},
		{
			Name: "Bool",
			In:   true,
			Exp: &types.AttributeValueMemberBOOL{
				Value: true,
			},
		},
		{
			Name: "Binary slice",
			In:   []byte{0, 1, 0, 1},
			Exp: &types.AttributeValueMemberB{
				Value: []byte{0, 1, 0, 1},
			},
		},
		{
			Name: "Binary matrix",
			In:   [][]byte{{0, 1, 1, 0}},
			Exp: &types.AttributeValueMemberBS{
				Value: [][]byte{{0, 1, 1, 0}},
			},
		},
		{
			Name: "Float32",
			In:   float32(3.1415),
			Exp: &types.AttributeValueMemberN{
				Value: "3.1415",
			},
		},
		{
			Name: "Float64",
			In:   3.1415,
			Exp: &types.AttributeValueMemberN{
				Value: "3.1415",
			},
		},
		{
			Name: "Int",
			In:   -99,
			Exp: &types.AttributeValueMemberN{
				Value: "-99",
			},
		},
		{
			Name: "Int8",
			In:   int8(-127),
			Exp: &types.AttributeValueMemberN{
				Value: "-127",
			},
		},
		{
			Name: "Int16",
			In:   int16(-32767),
			Exp: &types.AttributeValueMemberN{
				Value: "-32767",
			},
		},
		{
			Name: "Int32",
			In:   int32(-2147483647),
			Exp: &types.AttributeValueMemberN{
				Value: "-2147483647",
			},
		},
		{
			Name: "Int64",
			In:   int64(-9223372036854775807),
			Exp: &types.AttributeValueMemberN{
				Value: "-9223372036854775807",
			},
		},
		{
			Name: "Uint",
			In:   uint(99),
			Exp: &types.AttributeValueMemberN{
				Value: "99",
			},
		},
		{
			Name: "Uint8",
			In:   uint8(255),
			Exp: &types.AttributeValueMemberN{
				Value: "255",
			},
		},
		{
			Name: "Uint16",
			In:   uint16(65535),
			Exp: &types.AttributeValueMemberN{
				Value: "65535",
			},
		},
		{
			Name: "Uint32",
			In:   uint32(4294967295),
			Exp: &types.AttributeValueMemberN{
				Value: "4294967295",
			},
		},
		{
			Name: "Uint64",
			In:   uint64(18446744073709551615),
			Exp: &types.AttributeValueMemberN{
				Value: "18446744073709551615",
			},
		},
		{
			Name: "Time",
			In:   time.Date(2022, 1, 31, 12, 40, 30, 0, time.UTC),
			Exp: &types.AttributeValueMemberS{
				Value: "2022-01-31T12:40:30Z",
			},
		},
		{
			Name: "String set",
			In:   []string{"foo", "bar"},
			Exp: &types.AttributeValueMemberSS{
				Value: []string{"foo", "bar"},
			},
		},
		{
			Name: "Float32 set",
			In:   []float32{3.1415, 2.7},
			Exp: &types.AttributeValueMemberNS{
				Value: []string{"3.1415", "2.7"},
			},
		},
		{
			Name: "Float64 set",
			In:   []float64{3.1415, 2.7},
			Exp: &types.AttributeValueMemberNS{
				Value: []string{"3.1415", "2.7"},
			},
		},
		{
			Name: "Int set",
			In:   []int{99, -255},
			Exp: &types.AttributeValueMemberNS{
				Value: []string{"99", "-255"},
			},
		},
		{
			Name: "Uint set",
			In:   []uint{99, 255},
			Exp: &types.AttributeValueMemberNS{
				Value: []string{"99", "255"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			out := dynamoql.FormatAttribute(tt.In)
			assert.EqualValues(t, tt.Exp, out)
		})
	}
}

func TestParseBinary(t *testing.T) {
	exp := []byte{0, 1, 0}
	out := dynamoql.ParseBinary(&types.AttributeValueMemberB{
		Value: exp,
	})
	assert.Equal(t, exp, out)
}

func TestParseBinarySet(t *testing.T) {
	exp := [][]byte{{0, 1, 0}}
	out := dynamoql.ParseBinarySet(&types.AttributeValueMemberBS{
		Value: exp,
	})
	assert.Equal(t, exp, out)
}

func TestParseBool(t *testing.T) {
	out := dynamoql.ParseBool(&types.AttributeValueMemberBOOL{
		Value: true,
	})
	assert.True(t, out)
}

func TestParseFloat32(t *testing.T) {
	exp := float32(3.1415)
	out := dynamoql.ParseFloat32(&types.AttributeValueMemberN{
		Value: "3.1415",
	})
	assert.Equal(t, exp, out)
}

func TestParseFloat64(t *testing.T) {
	exp := 3.1415
	out := dynamoql.ParseFloat64(&types.AttributeValueMemberN{
		Value: "3.1415",
	})
	assert.Equal(t, exp, out)
}

func TestParseInt(t *testing.T) {
	exp := -99
	out := dynamoql.ParseInt(&types.AttributeValueMemberN{
		Value: "-99",
	})
	assert.Equal(t, exp, out)
}

func TestParseInt8(t *testing.T) {
	exp := int8(-127)
	out := dynamoql.ParseInt8(&types.AttributeValueMemberN{
		Value: "-127",
	})
	assert.Equal(t, exp, out)
}

func TestParseInt16(t *testing.T) {
	exp := int16(-32767)
	out := dynamoql.ParseInt16(&types.AttributeValueMemberN{
		Value: "-32767",
	})
	assert.Equal(t, exp, out)
}

func TestParseInt32(t *testing.T) {
	exp := int32(-2147483647)
	out := dynamoql.ParseInt32(&types.AttributeValueMemberN{
		Value: "-2147483647",
	})
	assert.Equal(t, exp, out)
}

func TestParseInt64(t *testing.T) {
	exp := int64(-9223372036854775807)
	out := dynamoql.ParseInt64(&types.AttributeValueMemberN{
		Value: "-9223372036854775807",
	})
	assert.Equal(t, exp, out)
}

func TestParseUint(t *testing.T) {
	exp := uint(99)
	out := dynamoql.ParseUint(&types.AttributeValueMemberN{
		Value: "99",
	})
	assert.Equal(t, exp, out)
}

func TestParseUint8(t *testing.T) {
	exp := uint8(255)
	out := dynamoql.ParseUint8(&types.AttributeValueMemberN{
		Value: "255",
	})
	assert.Equal(t, exp, out)
}

func TestParseUint16(t *testing.T) {
	exp := uint16(65535)
	out := dynamoql.ParseUint16(&types.AttributeValueMemberN{
		Value: "65535",
	})
	assert.Equal(t, exp, out)
}

func TestParseUint32(t *testing.T) {
	exp := uint32(4294967295)
	out := dynamoql.ParseUint32(&types.AttributeValueMemberN{
		Value: "4294967295",
	})
	assert.Equal(t, exp, out)
}

func TestParseUint64(t *testing.T) {
	exp := uint64(18446744073709551615)
	out := dynamoql.ParseUint64(&types.AttributeValueMemberN{
		Value: "18446744073709551615",
	})
	assert.Equal(t, exp, out)
}

func TestParseStringSet(t *testing.T) {
	exp := []string{"foo", "bar", "baz"}
	out := dynamoql.ParseStringSet(&types.AttributeValueMemberSS{
		Value: exp,
	})
	assert.Equal(t, exp, out)
}

func TestParseFloat32Set(t *testing.T) {
	exp := []float32{.1, -9.99}
	out := dynamoql.ParseFloat32Set(&types.AttributeValueMemberNS{
		Value: []string{".1", "-9.99"},
	})
	assert.Equal(t, exp, out)
}

func TestParseFloat64Set(t *testing.T) {
	exp := []float64{.1, -9.99}
	out := dynamoql.ParseFloat64Set(&types.AttributeValueMemberNS{
		Value: []string{".1", "-9.99"},
	})
	assert.Equal(t, exp, out)
}

func TestParseIntSet(t *testing.T) {
	exp := []int{1, -99}
	out := dynamoql.ParseIntSet(&types.AttributeValueMemberNS{
		Value: []string{"1", "-99"},
	})
	assert.Equal(t, exp, out)
}

func TestParseUintSet(t *testing.T) {
	exp := []uint{1, 99}
	out := dynamoql.ParseUintSet(&types.AttributeValueMemberNS{
		Value: []string{"1", "99"},
	})
	assert.Equal(t, exp, out)
}

func BenchmarkFormatAttributeString(b *testing.B) {
	f := "foo"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dynamoql.FormatAttribute(f)
	}
}

func BenchmarkFormatAttributeTime(b *testing.B) {
	f := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dynamoql.FormatAttribute(f)
	}
}
