package dynamoql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageToken_Encode(t *testing.T) {
	tests := []struct {
		Name  string
		Token dynamoql.PageToken
		Exp   string
	}{
		{
			Name:  "Empty",
			Token: nil,
			Exp:   "",
		},
		{
			Name: "Valid Partition Key only String",
			Token: dynamoql.PageToken{
				"author_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
			},
			Exp: "U35hdXRob3JfaWR-MTIzLWFiYw==",
		},
		{
			Name: "Valid Partition Key only Binary",
			Token: dynamoql.PageToken{
				"author_id": &types.AttributeValueMemberB{
					Value: []byte("123-abc"),
				},
			},
			Exp: "Qn5hdXRob3JfaWR-MTIzLWFiYw==",
		},
		{
			Name: "Valid Partition Key only Number",
			Token: dynamoql.PageToken{
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "Tn50aW1lc3RhbXBfdW5peH4xMjM=",
		},
		{
			Name: "Valid Composite Key",
			Token: dynamoql.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "U351c2VyX2lkfjEyMy1hYmMmTn50aW1lc3RhbXBfdW5peH4xMjM=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp := tt.Token.Encode()
			assert.Equal(t, tt.Exp, exp)
		})
	}
}

func TestPageToken_Decode(t *testing.T) {
	tests := []struct {
		Name  string
		Token dynamoql.PageToken
		Exp   string
	}{
		{
			Name:  "Empty",
			Token: dynamoql.PageToken{},
			Exp:   "",
		},
		{
			Name: "Valid Partition Key only String",
			Token: dynamoql.PageToken{
				"author_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
			},
			Exp: "U35hdXRob3JfaWR-MTIzLWFiYw==",
		},
		{
			Name: "Valid Partition Key only Binary",
			Token: dynamoql.PageToken{
				"author_id": &types.AttributeValueMemberB{
					Value: []byte("123-abc"),
				},
			},
			Exp: "Qn5hdXRob3JfaWR-MTIzLWFiYw==",
		},
		{
			Name: "Valid Partition Key only Number",
			Token: dynamoql.PageToken{
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "Tn50aW1lc3RhbXBfdW5peH4xMjM=",
		},
		{
			Name: "Valid Composite Key",
			Token: dynamoql.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "U351c2VyX2lkfjEyMy1hYmMmTn50aW1lc3RhbXBfdW5peH4xMjM=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			token := dynamoql.PageToken{}
			err := token.Decode(tt.Exp)
			require.NoError(t, err)
			assert.EqualValues(t, tt.Token, token)
		})
	}
}

func BenchmarkPageToken_Encode(b *testing.B) {
	token := dynamoql.PageToken{
		"user_id": &types.AttributeValueMemberS{
			Value: "123-abc",
		},
		"timestamp_unix": &types.AttributeValueMemberN{
			Value: "123",
		},
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = token.Encode()
	}
}

func BenchmarkPageToken_Decode(b *testing.B) {
	token := dynamoql.PageToken{
		"user_id": &types.AttributeValueMemberS{
			Value: "123-abc",
		},
		"timestamp_unix": &types.AttributeValueMemberN{
			Value: "123",
		},
	}
	data := token.Encode()
	tokenB := dynamoql.PageToken{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = tokenB.Decode(data)
	}
}
