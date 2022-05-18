package dynamoql

// took from: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

// ConditionalOperator Used to compare an operand against a range of values or an enumerated list of values.
type ConditionalOperator string

// LogicalOperator Used to perform logical evaluations (AND, OR & NOT).
type LogicalOperator string

// Ordering Used to set the traversing order while iterating items in a DynamoDB table.
type Ordering string

const (
	Equals             ConditionalOperator = "="
	GreaterThan        ConditionalOperator = ">"
	GreaterOrEqualThan ConditionalOperator = ">="
	LessThan           ConditionalOperator = "<"
	LessOrEqualThan    ConditionalOperator = "<="
	GreaterOrLess      ConditionalOperator = "<>"
	In                 ConditionalOperator = "IN"
	Between            ConditionalOperator = "BETWEEN"
	Contains           ConditionalOperator = "contains"
	BeginsWith         ConditionalOperator = "begins_with"
	AttributeType      ConditionalOperator = "attribute_type"
	AttributeExists    ConditionalOperator = "attribute_exists"
	AttributeNotExists ConditionalOperator = "attribute_not_exists"
	Size               ConditionalOperator = "size"

	And LogicalOperator = "AND"
	Or  LogicalOperator = "OR"

	Ascend  Ordering = "ASC"
	Descend Ordering = "DESC"
)
