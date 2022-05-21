package dynamoql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExpressionBuilderFunc(t *testing.T) {
	tests := []struct {
		Name     string
		Operator ConditionalOperator
		Args     expressionBuilderFuncArgs
		Exp      string
	}{
		{
			Name:     "Missing operator",
			Operator: "",
			Args:     expressionBuilderFuncArgs{},
			Exp:      "",
		},
		{
			Name:     "Missing field",
			Operator: GreaterOrEqualThan,
			Args:     expressionBuilderFuncArgs{},
			Exp:      "",
		},
		{
			Name:     "Default",
			Operator: GreaterOrEqualThan,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "#foo >= :foo",
		},
		{
			Name:     "Between",
			Operator: Between,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "#foo BETWEEN :foo AND :foo0",
		},
		{
			Name:     "In",
			Operator: In,
			Args: expressionBuilderFuncArgs{
				field:         "foo",
				totalExtraVal: 3,
			},
			Exp: "#foo IN (:foo,:foo0,:foo1,:foo2)",
		},
		{
			Name:     "In no extra values",
			Operator: In,
			Args: expressionBuilderFuncArgs{
				field:         "foo",
				totalExtraVal: 0,
			},
			Exp: "#foo IN (:foo)",
		},
		{
			Name:     "Attribute not exists",
			Operator: AttributeNotExists,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "attribute_not_exists(#foo)",
		},
		{
			Name:     "Attribute exists",
			Operator: AttributeExists,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "attribute_exists(#foo)",
		},
		{
			Name:     "Function begins_with",
			Operator: BeginsWith,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "begins_with(#foo,:foo)",
		},
		{
			Name:     "Function contains",
			Operator: Contains,
			Args: expressionBuilderFuncArgs{
				field:         "foo",
				totalExtraVal: 10,
			},
			Exp: "contains(#foo,:foo)",
		},
		{
			Name:     "Function attribute_type",
			Operator: AttributeType,
			Args: expressionBuilderFuncArgs{
				field: "foo",
			},
			Exp: "attribute_type(#foo,:foo)",
		},
		{
			Name:     "Function size",
			Operator: Size,
			Args: expressionBuilderFuncArgs{
				field:             "foo",
				secondaryOperator: LessOrEqualThan,
			},
			Exp: "size(#foo) <= :foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			buf := strings.Builder{}
			tt.Args.operator = tt.Operator
			newExpressionBuilderFunc(tt.Operator)(&buf, tt.Args)
			assert.Equal(t, tt.Exp, buf.String())
		})
	}
}

func BenchmarkNewExpressionBuilderFunc(b *testing.B) {
	// No longer allocates buffer capacity before writes.
	// Pre-allocation is done in BuildExpression().
	rootOp := In
	args := expressionBuilderFuncArgs{
		field:             "foo",
		operator:          rootOp,
		secondaryOperator: "",
		totalExtraVal:     100, // max values accepted by IN operator
	}
	for i := 0; i < b.N; i++ {
		buf := strings.Builder{}
		b.ReportAllocs()
		newExpressionBuilderFunc(rootOp)(&buf, args)
	}
}

func TestBuildExpression(t *testing.T) {
	conditions := []Condition{
		{
			Negate:            true,
			IsKey:             false,
			Operator:          In,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo"},
		},
		{
			Negate:            true,
			IsKey:             false,
			Operator:          In,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo", "peace"},
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Equals,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       nil,
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Contains,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       nil,
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Size,
			SecondaryOperator: GreaterOrEqualThan,
			Field:             "foo",
			Value:             nil,
			ExtraValues:       nil,
		},
	}
	out := buildExpression(And, false, conditions)
	require.NotNil(t, out)
	exp := "NOT (#foo IN (:foo,:foo0,:foo1,:foo2,:foo3)) AND NOT (#foo IN (:foo,:foo0,:foo1,:foo2,:foo3,:foo4)) AND #foo = :foo AND contains(#foo,:foo) AND size(#foo) >= :foo"
	assert.Equal(t, exp, *out)
}

func BenchmarkBuildExpression(b *testing.B) {
	conditions := []Condition{
		{
			Negate:            true,
			IsKey:             false,
			Operator:          In,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo"},
		},
		{
			Negate:            true,
			IsKey:             false,
			Operator:          In,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo"},
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Equals,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       nil,
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Contains,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             nil,
			ExtraValues:       nil,
		},
	}
	for i := 0; i < b.N; i++ {
		b.ReportAllocs()
		buildExpression(And, true, conditions)
	}
}
