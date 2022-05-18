package dynamoql_test

import (
	"testing"
	"time"

	"github.com/maestre3d/dynamoql"
)

func TestFormatAttribute(t *testing.T) {
	f := time.Now().UTC()
	out := dynamoql.FormatAttribute(f)
	t.Log(out)
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
