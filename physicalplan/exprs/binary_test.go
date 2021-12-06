package exprs

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/stretchr/testify/require"
	"query-engine/datatypes"
	"testing"
)

func TestBooleanExpr_AndExpr(t *testing.T) {
	// one recordBatch , two booleanExpr
	aBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int64Type)
	bBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int64Type)
	schema := datatypes.Schema{Fields: []datatypes.Field{{"Id", datatypes.Int64Type}}}
	aBuilder.AppendValues(int64(1), int64(1), int64(-1))
	bBuilder.AppendValues(int64(1), int64(2), int64(-1))
	recordBatch := datatypes.RecordBatch{
		Schema: schema,
		Fields: []datatypes.ColumnArray{aBuilder.Build(), bBuilder.Build()},
	}

	// eqExpr	(l == r)
	eqExpr := NewEqExpr(NewColumnExpr(0), NewColumnExpr(1))

	// gtExpr	(l > 0)
	gtExpr := NewGtExpr(NewColumnExpr(0), NewLiteralLongExpr(0))

	// andExpr	(col0 == col1 && col0 > 0)
	andExpr := NewAndExpr(eqExpr, gtExpr)

	expect := []bool{true, false, false}
	evalRes := andExpr.Evaluate(recordBatch)
	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], evalRes.GetValue(i), fmt.Sprintf("current index : %d", i))
	}
}

func TestMathExpr_Expr(t *testing.T) {
	// one recordBatch , two booleanExpr
	aBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int64Type)
	bBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int64Type)
	schema := datatypes.Schema{Fields: []datatypes.Field{{"Num", datatypes.Int64Type}}}
	aBuilder.AppendValues(int64(1), int64(2), int64(3))
	bBuilder.AppendValues(int64(1), int64(2), int64(3))
	recordBatch := datatypes.RecordBatch{
		Schema: schema,
		Fields: []datatypes.ColumnArray{aBuilder.Build(), bBuilder.Build()},
	}

	// addExpr => 2, 4, 6
	addExpr := NewAddExpr(NewColumnExpr(0), NewColumnExpr(1))

	// multiply => 4, 8, 12
	multiplyExpr := NewMultiplyExpr(addExpr, NewLiteralLongExpr(2))

	// subtract => 3, 7, 11
	lastExpr := NewSubtractExpr(multiplyExpr, NewLiteralLongExpr(1))

	expect := []int64{3, 7, 11}
	evalRes := lastExpr.Evaluate(recordBatch)
	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], evalRes.GetValue(i), fmt.Sprintf("current index : %d", i))
	}
}
