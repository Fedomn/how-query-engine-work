package datatypes

import (
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntArray(t *testing.T) {
	mem := memory.NewGoAllocator()
	int8Builder := array.NewInt8Builder(mem)
	defer int8Builder.Release()

	int8Builder.AppendValues(
		[]int8{1, 2, 3},
		[]bool{true, true, false},
	)

	arr := int8Builder.NewInt8Array()
	require.Equal(t, arr.Int8Values(), []int8{1, 2, 3})
	require.True(t, arr.IsNull(2))
}

func TestArrowArrayBuilder_Set(t *testing.T) {
	builder := NewArrowArrayBuilder(memory.NewGoAllocator(), &Int8Type{})
	size := 10
	for i := 0; i < size; i++ {
		builder.Append(int8(i))
	}
	res := builder.Build()
	for i := 0; i < size; i++ {
		require.Equal(t, int8(i), res.GetValue(i))
	}
}
