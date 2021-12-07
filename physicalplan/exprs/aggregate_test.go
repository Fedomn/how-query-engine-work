package exprs

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAggregate_Min(t *testing.T) {
	accumulator := NewMinExpr(NewColumnExpr(0)).createAccumulator()
	accumulator.accumulate(int8(10))
	accumulator.accumulate(int8(3))
	accumulator.accumulate(int8(5))
	require.Equal(t, int8(3), accumulator.finalValue())
}

func TestAggregate_Max(t *testing.T) {
	accumulator := NewMaxExpr(NewColumnExpr(0)).createAccumulator()
	accumulator.accumulate(int8(10))
	accumulator.accumulate(int8(3))
	accumulator.accumulate(int8(5))
	require.Equal(t, int8(10), accumulator.finalValue())
}

func TestAggregate_Sum(t *testing.T) {
	accumulator := NewSumExpr(NewColumnExpr(0)).createAccumulator()
	accumulator.accumulate(int8(10))
	accumulator.accumulate(int8(3))
	accumulator.accumulate(int8(5))
	require.Equal(t, int8(18), accumulator.finalValue())
}
