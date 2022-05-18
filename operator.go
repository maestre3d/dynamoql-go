package dynamoql

type ConditionalOperator string
type LogicalOperator string

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
	Contains           ConditionalOperator = "CONTAINS"
	BeginsWith         ConditionalOperator = "BEGINS_WITH"

	And LogicalOperator = "AND"
	Or  LogicalOperator = "OR"

	Ascend  Ordering = "ASC"
	Descend Ordering = "DESC"
)
