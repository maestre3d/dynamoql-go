package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSafeStringPtr(t *testing.T) {
	val := "foo"
	tests := []struct {
		name string
		in   string
		exp  *string
	}{
		{
			name: "Empty",
			in:   "",
			exp:  nil,
		},
		{
			name: "With data",
			in:   val,
			exp:  &val,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := newSafeStringPtr(tt.in)
			assert.EqualValues(t, tt.exp, out)
		})
	}
}
