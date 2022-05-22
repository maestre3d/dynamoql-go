package dynamoql

import (
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	expressionNameSeparator  = "#"
	expressionValueSeparator = ":"
)

// buildExpressionValuesRaw crafts a ExpressionValue using raw key names (does not requires ExpressionNames).
func buildExpressionValuesRaw(c []Condition) map[string]types.AttributeValue {
	if c == nil || len(c) == 0 {
		return nil
	}
	buf := make(map[string]types.AttributeValue, len(c))
	for i := range c {
		if val := FormatAttribute(c[i].Value); val != nil {
			buf[c[i].Field] = val
		}
	}
	return buf
}

// expression Amazon DynamoDB payload to execute specified filters and queries.
type expression struct {
	Names            map[string]string
	Values           map[string]types.AttributeValue
	KeyExpression    *string
	FilterExpression *string
}

// expressionBuilder crafts a DynamoDB expression.
type expressionBuilder struct {
	negate              bool
	operator            LogicalOperator
	conditions          []Condition
	conditionsKeys      []Condition
	conditionsAttr      []Condition
	expressionNamesBuf  map[string]string
	expressionValuesBuf map[string]types.AttributeValue
	expressionKey       *string
	expressionFilter    *string
}

// newExpression builds an expression from the given data.
func newExpression(op LogicalOperator, negate bool, c []Condition) expression {
	b := &expressionBuilder{
		negate:     negate,
		operator:   op,
		conditions: c,
	}
	return b.build()
}

// splitConditions separates Condition Keys and Condition Attributes.
//
// ConditionKeys are used by Amazon DynamoDB to execute queries related to Partition and Sort keys.
//
// AttributeKeys are used by Amazon DynamoDB to execute queries related to non-primary key fields.
func (b *expressionBuilder) splitConditions() {
	if b.conditions == nil || len(b.conditions) == 0 {
		return
	}
	for i := range b.conditions {
		if b.conditions[i].IsKey {
			b.conditionsKeys = append(b.conditionsKeys, b.conditions[i])
			continue
		}
		b.conditionsAttr = append(b.conditionsAttr, b.conditions[i])
	}
}

// buildNames maps field names from an Amazon DynamoDB table to a custom token used by expressions.
//
// Default format is key => #key.
func (b *expressionBuilder) buildNames() {
	if b.conditions == nil || len(b.conditions) == 0 {
		return
	}
	b.expressionNamesBuf = make(map[string]string, len(b.conditions))
	for i := range b.conditions {
		b.expressionNamesBuf[expressionNameSeparator+b.conditions[i].Field] =
			b.conditions[i].Field
	}
}

// buildValues maps values of an Amazon DynamoDB expression to a custom token.
//
// Default format is key => :key
//
// If more than one key, then => :key, :key0, :key1, :keyN ...
func (b *expressionBuilder) buildValues() {
	if b.conditions == nil || len(b.conditions) == 0 {
		return
	}
	b.expressionValuesBuf = make(map[string]types.AttributeValue, len(b.conditions))
	for i := range b.conditions {
		key := expressionValueSeparator + b.conditions[i].Field
		b.expressionValuesBuf[key] = FormatAttribute(b.conditions[i].Value)
		if b.conditions[i].ExtraValues == nil || len(b.conditions[i].ExtraValues) == 0 {
			continue
		}
		// Only applies to IN operator
		// O(mn) - Time complex. where m = extra values & n = total conditions.
		for j, attr := range b.conditions[i].ExtraValues {
			b.expressionValuesBuf[key+strconv.Itoa(j)] = FormatAttribute(attr)
		}
	}
}

type expressionBuilderFuncArgs struct {
	field                       string
	operator, secondaryOperator ConditionalOperator
	totalExtraVal               int
}

// expressionBuilderFunc based on the given arguments, it will build an Amazon DynamoDB expression (key or filter).
//
// This function writes bytes into the given strings.Builder buffer.
type expressionBuilderFunc func(buf *strings.Builder, args expressionBuilderFuncArgs)

// newExpressionBuilderFunc retrieves an expression builder function based on the given ConditionalOperator.
func newExpressionBuilderFunc(o ConditionalOperator) expressionBuilderFunc {
	switch o {
	case Between:
		return buildBetweenExpression
	case In:
		return buildInExpression
	case Size:
		return buildSizeExpression
	case AttributeExists, AttributeNotExists:
		return buildAttrExpression
	case BeginsWith, Contains, AttributeType:
		return buildFuncExpression
	default:
		return buildDefaultExpression
	}
}

func buildBetweenExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(' ')
	buf.WriteString(string(Between))
	buf.WriteByte(' ')
	buf.WriteString(expressionValueSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(' ')
	buf.WriteString(string(And))
	buf.WriteByte(' ')
	buf.WriteString(expressionValueSeparator)
	buf.WriteString(args.field)
	buf.WriteByte('0')
}

func buildInExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	key := expressionValueSeparator + args.field
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(' ')
	buf.WriteString(string(In))
	buf.WriteByte(' ')
	buf.WriteByte('(')
	buf.WriteString(key)
	if args.totalExtraVal > 0 {
		buf.WriteByte(',')
		for i := 0; i < args.totalExtraVal; i++ {
			buf.WriteString(key + strconv.Itoa(i))
			if args.totalExtraVal-1 != i {
				buf.WriteByte(',')
			}
		}
	}
	buf.WriteByte(')')
}

func buildSizeExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	buf.WriteString(string(Size))
	buf.WriteByte('(')
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(')')
	buf.WriteByte(' ')
	buf.WriteString(string(args.secondaryOperator))
	buf.WriteByte(' ')
	buf.WriteString(expressionValueSeparator)
	buf.WriteString(args.field)
}

func buildFuncExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	buf.WriteString(string(args.operator))
	buf.WriteByte('(')
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(',')
	buf.WriteString(expressionValueSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(')')
}

func buildAttrExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	buf.WriteString(string(args.operator))
	buf.WriteByte('(')
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(')')
}

func buildDefaultExpression(buf *strings.Builder, args expressionBuilderFuncArgs) {
	if args.field == "" {
		return
	}
	buf.WriteString(expressionNameSeparator)
	buf.WriteString(args.field)
	buf.WriteByte(' ')
	buf.WriteString(string(args.operator))
	buf.WriteByte(' ')
	buf.WriteString(expressionValueSeparator)
	buf.WriteString(args.field)
}

// calculateExpressionFuncCap calculates the required capacity for strings.Builder's internal bytes buffer.
//
// This will drastically reduce malloc and Bytes/op as the buffer won't require to be grown, hence potentially
// duplicating buffer's capacity for each grow is avoided.
//
// strings.Builder's Grow() method uses the formula:
// 2(len(buffer)) + n, where n is the requested growing factor, as capacity of the newly allocated buffer.
func calculateExpressionFuncCap(o ConditionalOperator, args expressionBuilderFuncArgs) int {
	totalExtraChars := 0
	switch o {
	case Between:
		totalExtraChars = 5
		return totalExtraChars + len(expressionNameSeparator) + (len(expressionValueSeparator) * 2) +
			len(Between) + len(And) + (len(args.field) * 3)
	case In:
		key := expressionValueSeparator + args.field
		totalExtraChars = 4

		requiredCap := totalExtraChars + len(expressionNameSeparator) + len(expressionValueSeparator) +
			len(In) + (len(args.field) * 2)
		if args.totalExtraVal > 0 {
			// buf.Grow uses the formula: 2(len(buffer)) + n to calculate new buffer capacity,
			// we no longer need to calculate exactly the bytes written.
			totalExtraChars = 2
			if args.totalExtraVal > 10 {
				totalExtraChars++
			}
			requiredCap += (len(key) + totalExtraChars) * args.totalExtraVal
			if args.totalExtraVal > 10 {
				requiredCap -= 10 // first 10 values do not contain double-digit, remove
			}
		}
		return requiredCap
	case Size:
		totalExtraChars = 4
		return totalExtraChars + len(expressionNameSeparator) + len(expressionValueSeparator) +
			len(Size) + (len(args.field) * 2) + len(args.secondaryOperator)
	case AttributeExists, AttributeNotExists:
		totalExtraChars = 2
		return totalExtraChars + len(expressionNameSeparator) +
			len(args.operator) + len(args.field)
	case BeginsWith, Contains, AttributeType:
		totalExtraChars = 3
		return totalExtraChars + len(expressionNameSeparator) + len(expressionValueSeparator) +
			len(args.operator) + (len(args.field) * 2)
	default:
		if args.field == "" {
			return 0
		}
		totalExtraChars = 2
		return totalExtraChars + len(expressionNameSeparator) + len(expressionValueSeparator) +
			len(args.operator) + (len(args.field) * 2)
	}
}

func calculateExpressionBufferCap(operator LogicalOperator, negate bool, c []Condition) int {
	const whiteSpaces = 2
	bufSize := 0
	if len(c) > 1 {
		// f(x) outputs total bytes concatenated for each logical operator.
		// (e.g. #foo = :foo AND #bar >= :bar => f(x) = 5)
		// (e.g. #foo = :foo OR #bar >= :bar OR #baz contains(:bar) => f(x) = 8)
		// Tc = Total Conditions
		// Ol = Logical Operator Length (either AND or operator OR)
		// W = Total Whitespaces
		// f(x) = Tc(W+Ol) - (Ol+W)
		bufSize = len(c)*(whiteSpaces+len(operator)) - (len(operator) + whiteSpaces)
	}
	if negate {
		// represents 'NOT ()'
		bufSize += 6
	}
	for i := range c {
		if !c[i].IsKey && c[i].Negate {
			bufSize += 6
		}
		bufSize += calculateExpressionFuncCap(c[i].Operator, expressionBuilderFuncArgs{
			field:             c[i].Field,
			operator:          c[i].Operator,
			secondaryOperator: c[i].SecondaryOperator,
			totalExtraVal:     len(c[i].ExtraValues),
		})
	}
	return bufSize
}

func buildExpression(operator LogicalOperator, negate bool, c []Condition) *string {
	if len(c) == 0 {
		return nil
	}
	// took reference from:
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
	buf := strings.Builder{}
	buf.Grow(calculateExpressionBufferCap(operator, negate, c))
	if negate {
		buf.WriteString(string(not))
		buf.WriteString(" (")
	}
	for i := range c {
		if !c[i].IsKey && c[i].Negate {
			buf.WriteString(string(not))
			buf.WriteString(" (")
		}
		newExpressionBuilderFunc(c[i].Operator)(&buf, expressionBuilderFuncArgs{
			field:             c[i].Field,
			operator:          c[i].Operator,
			secondaryOperator: c[i].SecondaryOperator,
			totalExtraVal:     len(c[i].ExtraValues),
		})
		if !c[i].IsKey && c[i].Negate {
			buf.WriteByte(')')
		}
		if len(c)-1 > i {
			buf.WriteByte(' ')
			buf.WriteString(string(operator))
			buf.WriteByte(' ')
		}
	}
	if negate {
		buf.WriteByte(')')
	}
	if buf.Len() == 0 {
		return nil
	}
	return aws.String(buf.String())
}

func (b *expressionBuilder) buildKeys() {
	// Key expressions DO NOT accept any LogicalOperator except And
	b.expressionKey = buildExpression(And, false, b.conditionsKeys)
}

func (b *expressionBuilder) buildFilters() {
	b.expressionFilter = buildExpression(b.operator, b.negate, b.conditionsAttr)
}

func (b *expressionBuilder) build() expression {
	b.buildNames()
	b.buildValues()
	b.splitConditions()
	b.buildKeys()
	b.buildFilters()
	return expression{
		Names:            b.expressionNamesBuf,
		Values:           b.expressionValuesBuf,
		KeyExpression:    b.expressionKey,
		FilterExpression: b.expressionFilter,
	}
}
