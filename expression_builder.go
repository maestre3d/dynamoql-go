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
		buf[c[i].Field] = FormatAttribute(c[i].Value)
	}
	return buf
}

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

func newExpressionBuilder(op LogicalOperator, negate bool, c []Condition) *expressionBuilder {
	b := &expressionBuilder{
		negate:     negate,
		operator:   op,
		conditions: c,
	}
	b.build()
	return b
}

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

func (b *expressionBuilder) buildExpressionNames() {
	if b.conditions == nil || len(b.conditions) == 0 {
		return
	}
	b.expressionNamesBuf = make(map[string]string, len(b.conditions))
	for i := range b.conditions {
		b.expressionNamesBuf[expressionNameSeparator+b.conditions[i].Field] =
			b.conditions[i].Field
	}
}

func (b *expressionBuilder) buildExpressionValues() {
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
		if b.conditions[i].Operator == Between {
			key += "0"
			b.expressionValuesBuf[key] = FormatAttribute(b.conditions[i].ExtraValues[0])
		} else if b.conditions[i].Operator == In {
			for j, attr := range b.conditions[i].ExtraValues {
				key += strconv.Itoa(j)
				b.expressionValuesBuf[key] = FormatAttribute(attr)
			}
		}
	}
}

func buildExpression(operator LogicalOperator, negate bool, c []Condition) *string {
	// took reference from:
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
	buf := strings.Builder{}
	if negate {
		buf.WriteString("NOT (")
	}
	for i := range c {
		if !c[i].IsKey && c[i].Negate {
			buf.WriteString("NOT (")
		}
		switch c[i].Operator {
		case Between:
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteString(" " + string(Between) + " ")
			buf.WriteString(expressionValueSeparator + c[i].Field)
			buf.WriteString(" " + string(And) + " ")
			buf.WriteString(expressionValueSeparator + c[i].Field + "0")
		case In:
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteString(" " + string(In) + " (")
			key := expressionValueSeparator + c[i].Field
			buf.WriteString(key)
			if c[i].ExtraValues == nil || len(c[i].ExtraValues) == 0 {
				continue
			}
			buf.WriteByte(',')
			for j := range c[i].ExtraValues {
				key += strconv.Itoa(j)
				buf.WriteString(key)
				if len(c[i].ExtraValues)-1 != j {
					buf.WriteByte(',')
				}
			}
			buf.WriteByte(')')
		case Size:
			buf.WriteString(string(c[i].Operator))
			buf.WriteByte('(')
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteString(") ")
			buf.WriteString(string(c[i].SecondaryOperator))
			buf.WriteByte(' ')
			buf.WriteString(expressionValueSeparator + c[i].Field)
		case AttributeExists, AttributeNotExists:
			buf.WriteString(string(c[i].Operator))
			buf.WriteByte('(')
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteByte(')')
		case BeginsWith, Contains, AttributeType:
			buf.WriteString(string(c[i].Operator))
			buf.WriteByte('(')
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteByte(',')
			buf.WriteString(expressionValueSeparator + c[i].Field)
			buf.WriteByte(')')
		default:
			buf.WriteString(expressionNameSeparator + c[i].Field)
			buf.WriteByte(' ')
			buf.WriteString(string(c[i].Operator))
			buf.WriteByte(' ')
			buf.WriteString(expressionValueSeparator + c[i].Field)
		}
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
	return aws.String(buf.String())
}

func (b *expressionBuilder) buildKeyExpression() {
	// Key expressions DO NOT accept any LogicalOperator except And
	b.expressionKey = buildExpression(And, false, b.conditionsKeys)
}

func (b *expressionBuilder) buildFilterExpression() {
	b.expressionFilter = buildExpression(b.operator, b.negate, b.conditionsAttr)
}

func (b *expressionBuilder) build() {
	b.buildExpressionNames()
	b.buildExpressionValues()
	b.splitConditions()
	b.buildKeyExpression()
	b.buildFilterExpression()
}
