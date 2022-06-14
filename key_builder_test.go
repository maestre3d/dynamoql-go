package dynamoql_test

import (
	"testing"

	"github.com/maestre3d/dynamoql"
	"github.com/stretchr/testify/assert"
)

func TestNewCompositeKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		v      string
		exp    string
	}{
		{
			name:   "Empty",
			prefix: "",
			v:      "",
			exp:    "",
		},
		{
			name:   "Empty v",
			prefix: "FOO",
			v:      "",
			exp:    "FOO" + dynamoql.CompositeKeySeparator,
		},
		{
			name:   "Missing Prefix",
			prefix: "",
			v:      "123",
			exp:    dynamoql.CompositeKeySeparator + "123",
		},
		{
			name:   "Valid value",
			prefix: "FOO",
			v:      "123",
			exp:    "FOO" + dynamoql.CompositeKeySeparator + "123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp := dynamoql.NewCompositeKey(tt.prefix, tt.v)
			assert.Equal(t, tt.exp, exp)
		})
	}
}

func TestParseCompositeKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		exp  string
	}{
		{
			name: "Empty",
			key:  "",
			exp:  "",
		},
		{
			name: "Two sep",
			key:  "FOO" + dynamoql.CompositeKeySeparator + "123" + dynamoql.CompositeKeySeparator,
			exp:  "123#",
		},
		{
			name: "Missing prefix",
			key:  dynamoql.CompositeKeySeparator + "123",
			exp:  "123",
		},
		{
			name: "Missing separator",
			key:  "FOO123",
			exp:  "FOO123",
		},
		{
			name: "Key only value",
			key:  "123",
			exp:  "123",
		},
		{
			name: "Valid value",
			key:  "FOO" + dynamoql.CompositeKeySeparator + "123",
			exp:  "123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp := dynamoql.ParseCompositeKey(tt.key)
			assert.Equal(t, tt.exp, exp)
		})
	}
}

func TestNewOverloadedKey(t *testing.T) {
	tests := []struct {
		name string
		sep  string
		v    []string
		exp  string
	}{
		{
			name: "Empty",
			sep:  "",
			v:    nil,
			exp:  "",
		},
		{
			name: "Nil v",
			sep:  ":",
			v:    nil,
			exp:  "",
		},
		{
			name: "Empty v",
			sep:  ":",
			v:    []string{},
			exp:  "",
		},
		{
			name: "Missing sep",
			sep:  "",
			v:    []string{"London"},
			exp:  "",
		},
		{
			name: "Missing sep multi value",
			sep:  "",
			v:    []string{"London", "Westminster", "Downing St", "10"},
			exp:  "",
		},
		{
			name: "Valid single value",
			sep:  ":",
			v:    []string{"London"},
			exp:  "London",
		},
		{
			name: "Valid multi value",
			sep:  ":",
			v:    []string{"London", "Westminster", "Downing St", "10"},
			exp:  "London:Westminster:Downing St:10",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp := dynamoql.NewOverloadedKey(tt.sep, tt.v...)
			assert.Equal(t, tt.exp, exp)
		})
	}
}

func BenchmarkBuildOverloadedKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dynamoql.NewOverloadedKey(":", "London", "Westminster", "Downing St", "10")
	}
}
