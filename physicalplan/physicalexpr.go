package physicalplan

import "query-engine/datatypes"

// PhysicalExpr containing the code to evaluate the expressions at runtime.
type PhysicalExpr interface {
	// Evaluate the expression against an input RecordBatch and
	// produce a column of data as output.
	Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray
}
