package exprs

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAggregate_Min(t *testing.T) {
	accumulator := NewMinExpr(NewColumnExpr(0)).CreateAccumulator()
	accumulator.Accumulate(int8(10))
	accumulator.Accumulate(int8(3))
	accumulator.Accumulate(int8(5))
	require.Equal(t, int8(3), accumulator.FinalValue())
}

func TestAggregate_Max(t *testing.T) {
	accumulator := NewMaxExpr(NewColumnExpr(0)).CreateAccumulator()
	accumulator.Accumulate(int8(10))
	accumulator.Accumulate(int8(3))
	accumulator.Accumulate(int8(5))
	require.Equal(t, int8(10), accumulator.FinalValue())
}

func TestAggregate_Sum(t *testing.T) {
	accumulator := NewSumExpr(NewColumnExpr(0)).CreateAccumulator()
	accumulator.Accumulate(int8(10))
	accumulator.Accumulate(int8(3))
	accumulator.Accumulate(int8(5))
	require.Equal(t, int8(18), accumulator.FinalValue())
}
