package dynamoql

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionValuesBuildRaw(t *testing.T) {
	type arbitrary struct{}

	tests := []struct {
		Name string
		In   []Condition
		Exp  map[string]types.AttributeValue
	}{
		{
			Name: "Nil",
			In:   nil,
			Exp:  nil,
		},
		{
			Name: "Empty",
			In:   []Condition{},
			Exp:  nil,
		},
		{
			Name: "Arbitrary value",
			In: []Condition{
				{
					Field: "foo",
					Value: arbitrary{},
				},
			},
			Exp: map[string]types.AttributeValue{},
		},
		{
			Name: "Arbitrary value, valid value",
			In: []Condition{
				{
					Field: "foo",
					Value: arbitrary{},
				},
				{
					Field: "bar",
					Value: "baz",
				},
			},
			Exp: map[string]types.AttributeValue{
				"bar": &types.AttributeValueMemberS{
					Value: "baz",
				},
			},
		},
		{
			Name: "One value",
			In: []Condition{
				{
					Field: "foo",
					Value: "bar",
				},
			},
			Exp: map[string]types.AttributeValue{
				"foo": &types.AttributeValueMemberS{
					Value: "bar",
				},
			},
		},
		{
			Name: "Two values, String and number",
			In: []Condition{
				{
					Field: "foo",
					Value: "bar",
				},
				{
					Field: "baz",
					Value: 20,
				},
			},
			Exp: map[string]types.AttributeValue{
				"foo": &types.AttributeValueMemberS{
					Value: "bar",
				},
				"baz": &types.AttributeValueMemberN{
					Value: "20",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			exp := buildExpressionValuesRaw(tt.In)
			assert.Equal(t, tt.Exp, exp)
		})
	}
}

func TestExpressionBuilderSplitConditions(t *testing.T) {
	tests := []struct {
		Name          string
		In            []Condition
		ExpLenKeys    int
		ExpFieldsKeys []string
		ExpLenAttr    int
		ExpFieldsAttr []string
	}{
		{
			Name:          "Empty",
			In:            nil,
			ExpLenKeys:    0,
			ExpFieldsKeys: nil,
			ExpLenAttr:    0,
			ExpFieldsAttr: nil,
		},
		{
			Name: "One key",
			In: []Condition{
				{
					Negate:            false,
					IsKey:             true,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "foo",
					Value:             nil,
					ExtraValues:       nil,
				},
			},
			ExpLenKeys:    1,
			ExpFieldsKeys: []string{"foo"},
			ExpLenAttr:    0,
			ExpFieldsAttr: nil,
		},
		{
			Name: "One attribute",
			In: []Condition{
				{
					Negate:            false,
					IsKey:             false,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "foo",
					Value:             nil,
					ExtraValues:       nil,
				},
			},
			ExpLenKeys:    0,
			ExpFieldsKeys: nil,
			ExpLenAttr:    1,
			ExpFieldsAttr: []string{"foo"},
		},
		{
			Name: "Two keys, one attribute",
			In: []Condition{
				{
					Negate:            false,
					IsKey:             true,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "foo",
					Value:             nil,
					ExtraValues:       nil,
				},
				{
					Negate:            false,
					IsKey:             true,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "bar",
					Value:             nil,
					ExtraValues:       nil,
				},
				{
					Negate:            false,
					IsKey:             false,
					Operator:          GreaterOrEqualThan,
					SecondaryOperator: "",
					Field:             "baz",
					Value:             nil,
					ExtraValues:       nil,
				},
			},
			ExpLenKeys:    2,
			ExpFieldsKeys: []string{"foo", "bar"},
			ExpLenAttr:    1,
			ExpFieldsAttr: []string{"baz"},
		},
		{
			Name: "Two keys, two attributes",
			In: []Condition{
				{
					Negate:            false,
					IsKey:             true,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "foo",
					Value:             nil,
					ExtraValues:       nil,
				},
				{
					Negate:            false,
					IsKey:             true,
					Operator:          Equals,
					SecondaryOperator: "",
					Field:             "bar",
					Value:             nil,
					ExtraValues:       nil,
				},
				{
					Negate:            false,
					IsKey:             false,
					Operator:          GreaterOrEqualThan,
					SecondaryOperator: "",
					Field:             "baz",
					Value:             nil,
					ExtraValues:       nil,
				},
				{
					Negate:            false,
					IsKey:             false,
					Operator:          LessOrEqualThan,
					SecondaryOperator: "",
					Field:             "foobar",
					Value:             nil,
					ExtraValues:       nil,
				},
			},
			ExpLenKeys:    2,
			ExpFieldsKeys: []string{"foo", "bar"},
			ExpLenAttr:    2,
			ExpFieldsAttr: []string{"baz", "foobar"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			b := expressionBuilder{
				conditions: tt.In,
			}
			require.Nil(t, b.conditionsKeys)
			require.Nil(t, b.conditionsAttr)
			b.splitConditions()
			require.Len(t, b.conditionsKeys, tt.ExpLenKeys)
			for i, v := range b.conditionsKeys {
				require.Equal(t, v.Field, tt.ExpFieldsKeys[i])
			}
			require.Len(t, b.conditionsAttr, tt.ExpLenAttr)
			for i, v := range b.conditionsAttr {
				require.Equal(t, v.Field, tt.ExpFieldsAttr[i])
			}
		})
	}
}

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
			Value:             "bar",
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo"},
		},
		{
			Negate:            true,
			IsKey:             false,
			Operator:          In,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             "bar",
			ExtraValues:       []interface{}{"foo", "bar", "baz", "bingo"},
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Equals,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             "bar",
			ExtraValues:       nil,
		},
		{
			Negate:            false,
			IsKey:             false,
			Operator:          Contains,
			SecondaryOperator: "",
			Field:             "foo",
			Value:             "abc",
			ExtraValues:       nil,
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buildExpression(And, true, conditions)
	}
}
