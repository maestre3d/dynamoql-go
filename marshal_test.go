package dynamoql_test

import (
	"testing"
	"time"

	"github.com/maestre3d/dynamoql"
	"github.com/stretchr/testify/require"
)

type book struct {
	AuthorID    string `dynamodb:"AuthorID,partition_key"`
	BookID      string
	Title       string `dynamodb:"DisplayName"`
	PublishDate time.Time
	Price       float64
	Stock       int
}

func TestMarshal(t *testing.T) {
	out, err := dynamoql.Marshal(book{
		AuthorID:    "123-abc",
		BookID:      "abc-456",
		Title:       "The Tale Of Us",
		PublishDate: time.Now(),
		Price:       199.99,
		Stock:       1320,
	})
	require.NoError(t, err)
	t.Log(out)
}

func BenchmarkMarshal(b *testing.B) {
	book := book{
		AuthorID:    "123-abc",
		BookID:      "abc-456",
		Title:       "The Tale Of Us",
		PublishDate: time.Now(),
		Price:       199.99,
		Stock:       1320,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = dynamoql.Marshal(book)
	}
}
