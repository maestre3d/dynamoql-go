package dynamoql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/maestre3d/dynamoql-go"
	"github.com/stretchr/testify/assert"
)

func TestNewItemBuffer(t *testing.T) {
	tests := []struct {
		name string
		size int
		grow int
	}{
		{
			name: "empty",
			size: 0,
			grow: 0,
		},
		{
			name: "copy grow",
			size: 10,
			grow: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := dynamoql.NewItemBuffer(tt.size)
			assert.Equal(t, 0, buf.Len())
			assert.Equal(t, tt.size, buf.Cap())
			assert.Empty(t, buf.Items())
			if tt.size == 0 {
				assert.True(t, buf.IsFull())
			} else {
				assert.False(t, buf.IsFull())
			}
			assert.False(t, buf.PeekAt(0))
			assert.Nil(t, buf.ItemAt(0))
			assert.Nil(t, buf.ItemAt(tt.size+1))

			buf.Write(map[string]types.AttributeValue{
				"foo": &types.AttributeValueMemberS{Value: "bar"},
			})
			buf.Grow(tt.grow)
			capacity := tt.grow + buf.Len()*2
			assert.Equal(t, capacity, buf.Cap())
			assert.Equal(t, 1, buf.Len())

			buf.WriteItems([]map[string]types.AttributeValue{
				{
					"foo": &types.AttributeValueMemberS{Value: "bar"},
				},
				{
					"baz": &types.AttributeValueMemberS{Value: "foo"},
				},
			})
			assert.True(t, buf.PeekAt(0))
			assert.NotNil(t, buf.ItemAt(0))

			buf.Reset()
			if tt.size == 0 {
				capacity *= 2 // using internal slice allocation
			}
			assert.Equal(t, capacity, buf.Cap())
			assert.Equal(t, 0, buf.Len())
			assert.False(t, buf.PeekAt(0))
			assert.Nil(t, buf.ItemAt(0))
		})
	}
}
