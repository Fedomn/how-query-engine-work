package physicalplan

import (
	"fmt"
	"query-engine/datatypes"
)

// PhysicalPlan represents an executable piece of code that will produce data.
type PhysicalPlan interface {
	// Schema Returns the schema of the data that will be produced by this physical plan.
	Schema() datatypes.Schema

	// Execute a physical plan and produce a series of record batches.
	Execute() datatypes.RecordBatch

	// Next prepares the next recordBatch for reading with Execute method
	Next() bool

	// Children Returns the children (inputs) of this physical plan.
	Children() []PhysicalPlan

	String() string
}

// PrettyFormat Format a physical plan in human-readable form
func PrettyFormat(plan PhysicalPlan) string {
	return "\n" + format(plan, 0)
}

func format(plan PhysicalPlan, indent int) string {
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
