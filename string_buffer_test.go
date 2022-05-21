package dynamoql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringBuffer(t *testing.T) {
	tests := []struct {
		Name       string
		InitialCap int
		Word       string
		NewWord    string
		FinalCap   int
	}{
		{
			Name:       "None",
			InitialCap: 0,
			Word:       "",
			NewWord:    "",
			FinalCap:   0,
		},
		{
			Name:       "No init length",
			InitialCap: 4,
			Word:       "",
			NewWord:    strings.Repeat("a", 14),
			FinalCap:   14,
		},
		{
			Name:       "No grow",
			InitialCap: 4,
			Word:       "ab",
			NewWord:    strings.Repeat("a", 2),
			FinalCap:   4,
		},
		{
			Name:       "Full",
			InitialCap: 2,
			Word:       "ab", // increases buffer's length
			NewWord:    strings.Repeat("a", 14),
			FinalCap:   16,
		},
		{
			Name:       "Full greater init cap",
			InitialCap: 4,
			Word:       "abcd", // increases buffer's length
			NewWord:    strings.Repeat("a", 3),
			FinalCap:   7,
		},
		{
			Name:       "Non full",
			InitialCap: 4,
			Word:       "ab",
			NewWord:    strings.Repeat("a", 14),
			FinalCap:   16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			buf := stringBuffer{}
			buf.Grow(tt.InitialCap)
			buf.WriteString(tt.Word)
			buf.WriteString(tt.NewWord)
			assert.Equal(t, tt.FinalCap, buf.Cap())
		})
	}
}

func BenchmarkStringBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := stringBuffer{}
		buf.Grow(2)
		b.ReportAllocs()
		buf.WriteString("abc")
	}
}
