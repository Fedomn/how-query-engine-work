package exprs

import (
	"fmt"
	"query-engine/datatypes"
)

// ---------------------------------------------Column Expressions---------------------------------------------

// ColumnExpr Column Reference column in a batch by index
type ColumnExpr struct {
	index int
}

func (c ColumnExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return input.Field(c.index)
}

func (c ColumnExpr) String() string {
	return fmt.Sprintf("#%d", c.index)
}

func NewColumnExpr(i int) ColumnExpr {
	return ColumnExpr{i}
}
