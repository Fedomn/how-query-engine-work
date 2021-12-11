package exprs

import (
	"fmt"
	"query-engine/datatypes"
)

// ---------------------------------------------Column Expressions---------------------------------------------

// ColumnIndexExpr Column Reference column in a batch by index
type ColumnIndexExpr struct {
	index int
}

func (c ColumnIndexExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return input.Field(c.index)
}

func (c ColumnIndexExpr) String() string {
	return fmt.Sprintf("#%d", c.index)
}

func NewColumnIndexExpr(i int) ColumnIndexExpr {
	return ColumnIndexExpr{i}
}
