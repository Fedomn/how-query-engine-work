package datatypes

import (
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
)

// ColumnArray Abstraction over different implementations of a column vector.
type ColumnArray interface {
	getType() arrow.DataType
	getValue(i int) interface{}
	size() int
}

type ArrowFieldArray struct {
	fieldArray array.Interface
}

func (a *ArrowFieldArray) getType() arrow.DataType {
	return a.fieldArray.DataType()
}

func (a *ArrowFieldArray) getValue(i int) interface{} {
	if a.fieldArray.IsNull(i) {
		return nil
	}
	switch v := a.fieldArray.(type) {
	case *array.Boolean:
		return v.Value(i)
	case *array.Int8:
		return v.Value(i)
	case *array.Int16:
		return v.Value(i)
	case *array.Int32:
		return v.Value(i)
	case *array.Int64:
		return v.Value(i)
	case *array.Uint8:
		return v.Value(i)
	case *array.Uint16:
		return v.Value(i)
	case *array.Uint32:
		return v.Value(i)
	case *array.Uint64:
		return v.Value(i)
	case *array.Float32:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.String:
		return v.Value(i)
	default:
		panic("invalid fieldArray type")
	}
}

func (a *ArrowFieldArray) size() int {
	return a.fieldArray.Len()
}

type LiteralValueArray struct {
	arrowType arrow.DataType
	value     interface{}
	arraySize int
}

func (l *LiteralValueArray) getType() arrow.DataType {
	return l.arrowType
}

func (l *LiteralValueArray) getValue(i int) interface{} {
	if i < 0 || i > l.arraySize {
		panic("index out of bound")
	}
	return l.value
}

func (l *LiteralValueArray) size() int {
	return l.arraySize
}
