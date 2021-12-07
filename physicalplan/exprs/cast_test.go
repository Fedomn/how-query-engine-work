package exprs

import (
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/stretchr/testify/require"
	"query-engine/datatypes"
	"testing"
)

func TestCastExpr_int8_to_string(t *testing.T) {
	aBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int8Type)
	schema := datatypes.Schema{Fields: []datatypes.Field{{"Id", datatypes.Int8Type}}}
	aBuilder.AppendValues(int8(1), int8(2), int8(-1))
	recordBatch := datatypes.RecordBatch{
		Schema: schema,
		Fields: []datatypes.ColumnArray{aBuilder.Build()},
	}

	expr := NewCastExpr(NewColumnExpr(0), datatypes.StringType)
	result := expr.Evaluate(recordBatch)

	expect := []string{"1", "2", "-1"}
	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], result.GetValue(i))
	}
}

func TestCastExpr_string_to_int8(t *testing.T) {
	aBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.StringType)
	schema := datatypes.Schema{Fields: []datatypes.Field{{"Id", datatypes.StringType}}}
	aBuilder.AppendValues("1", "2", "-1")
	recordBatch := datatypes.RecordBatch{
		Schema: schema,
		Fields: []datatypes.ColumnArray{aBuilder.Build()},
	}

	expr := NewCastExpr(NewColumnExpr(0), datatypes.Int8Type)
	result := expr.Evaluate(recordBatch)

	expect := []int8{int8(1), int8(2), int8(-1)}
	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], result.GetValue(i))
	}
}
