package dynamodb_go

type Operator string

type Ordering string

const (
	Equals             Operator = "="
	GreaterThan        Operator = ">"
	GreaterOrEqualThan Operator = ">="
	LessThan           Operator = "<"
	LessOrEqualThan    Operator = "<="
	GreaterOrLess      Operator = "<>"
	In                 Operator = "IN"
	Between            Operator = "BETWEEN"
	Contains           Operator = "CONTAINS"
	BeginsWith         Operator = "BEGINS_WITH"
	And                Operator = "AND"
	Or                 Operator = "OR"

	Ascend  Ordering = "ASC"
	Descend Ordering = "DESC"
)
