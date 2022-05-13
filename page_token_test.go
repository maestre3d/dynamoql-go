package dynamodb_go_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodb "github.com/maestre3d/dynamodb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageToken_Encode(t *testing.T) {
	tests := []struct {
		Name  string
		Token dynamodb.PageToken
		Exp   string
	}{
		{
			Name:  "Empty",
			Token: nil,
			Exp:   "",
		},
		{
			Name: "Valid Partition Key only String",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
			},
			Exp: "U3x1c2VyX2lkPTEyMy1hYmM=",
		},
		{
			Name: "Valid Partition Key only Binary",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberB{
					Value: []byte("123-abc"),
				},
			},
			Exp: "Qnx1c2VyX2lkPTEyMy1hYmM=",
		},
		{
			Name: "Valid Partition Key only Number",
			Token: dynamodb.PageToken{
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "Tnx0aW1lc3RhbXBfdW5peD0xMjM=",
		},
		{
			Name: "Valid Composite Key",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "U3x1c2VyX2lkPTEyMy1hYmMmTnx0aW1lc3RhbXBfdW5peD0xMjM=",
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
		Token dynamodb.PageToken
		Exp   string
	}{
		{
			Name:  "Empty",
			Token: dynamodb.PageToken{},
			Exp:   "",
		},
		{
			Name: "Valid Partition Key only String",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
			},
			Exp: "U3x1c2VyX2lkPTEyMy1hYmM=",
		},
		{
			Name: "Valid Partition Key only Binary",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberB{
					Value: []byte("123-abc"),
				},
			},
			Exp: "Qnx1c2VyX2lkPTEyMy1hYmM=",
		},
		{
			Name: "Valid Partition Key only Number",
			Token: dynamodb.PageToken{
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "Tnx0aW1lc3RhbXBfdW5peD0xMjM=",
		},
		{
			Name: "Valid Composite Key",
			Token: dynamodb.PageToken{
				"user_id": &types.AttributeValueMemberS{
					Value: "123-abc",
				},
				"timestamp_unix": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
			Exp: "U3x1c2VyX2lkPTEyMy1hYmMmTnx0aW1lc3RhbXBfdW5peD0xMjM=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			token := dynamodb.PageToken{}
			err := token.Decode(tt.Exp)
			require.NoError(t, err)
			assert.EqualValues(t, tt.Token, token)
		})
	}
}

func BenchmarkPageToken_Encode(b *testing.B) {
	token := dynamodb.PageToken{
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
	token := dynamodb.PageToken{
		"user_id": &types.AttributeValueMemberS{
			Value: "123-abc",
		},
		"timestamp_unix": &types.AttributeValueMemberN{
			Value: "123",
		},
	}
	data := token.Encode()
	tokenB := dynamodb.PageToken{}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		_ = tokenB.Decode(data)
	}
}
