package exprs

import (
	"fmt"
	"query-engine/datatypes"
)

// ---------------------------------------------Literal Expressions---------------------------------------------

type LiteralLongExpr struct {
	val int64
}

func (l LiteralLongExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(datatypes.Int64Type, l.val, input.RowCount())
}

func (l LiteralLongExpr) String() string {
	return fmt.Sprint(l.val)
}

func NewLiteralLongExpr(val int64) LiteralLongExpr {
	return LiteralLongExpr{val}
}

type LiteralDoubleExpr struct {
	val float64
}

func (l LiteralDoubleExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(datatypes.DoubleType, l.val, input.RowCount())
}

func (l LiteralDoubleExpr) String() string {
	return fmt.Sprint(l.val)
}

func NewLiteralDoubleExpr(val float64) LiteralDoubleExpr {
	return LiteralDoubleExpr{val}
}

type LiteralStringExpr struct {
	val string
}

func (l LiteralStringExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(datatypes.StringType, l.val, input.RowCount())
}

func (l LiteralStringExpr) String() string {
	return fmt.Sprintf("'%s'", l.val)
}

func NewLiteralStringExpr(val string) LiteralStringExpr {
	return LiteralStringExpr{val}
}
