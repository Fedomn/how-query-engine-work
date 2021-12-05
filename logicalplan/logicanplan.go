package logicalplan

import (
	"fmt"
	"query-engine/datatypes"
)

// LogicalPlan A logical plan represents a data transformation or action
// that returns a relation (a set of tuples).
type LogicalPlan interface {
	// Schema Returns the schema of the data that will be produced by this logical plan.
	Schema() datatypes.Schema

	// Children Returns the children (inputs) of this logical plan.
	Children() []LogicalPlan

	String() string
}

// PrettyFormat Format a logical plan in human-readable form
func PrettyFormat(plan LogicalPlan) string {
	return "\n" + format(plan, 0)
}

func format(plan LogicalPlan, indent int) string {
	str := ""
	for i := 0; i < indent; i++ {
		str += "\t"
	}
	str += fmt.Sprintf("%s\n", plan)
	for _, child := range plan.Children() {
		str += format(child, indent+1)
	}
	return str
}
