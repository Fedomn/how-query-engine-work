package logicalplan

import "query-engine/datatypes"

// LogicalExpr Logical Expression for use in logical query plans.
// The logical expression provides information needed during the planning phase
// such as the name and data type of the expression.
type LogicalExpr interface {
	// ToField Return meta-data about the value that will be produced
	// by this expression when evaluated against a particular input.
	ToField(input LogicalPlan) datatypes.Field

	String() string
}
