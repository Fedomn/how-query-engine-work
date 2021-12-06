package exprs

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"query-engine/datatypes"
	"query-engine/physicalplan"
)

// ---------------------------------------------Binary Expressions---------------------------------------------

type BinaryExprEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{}

type BinaryExpr struct {
	name     string
	op       string
	l, r     physicalplan.PhysicalExpr
	evalFunc BinaryExprEvalFunc
}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.l, b.op, b.r)
}

// -----------------Boolean Expressions-----------------

type BooleanExpr struct {
	BinaryExpr
}

func (b BooleanExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	ll := b.l.Evaluate(input)
	rr := b.r.Evaluate(input)
	if ll.Size() != rr.Size() || ll.GetType() != rr.GetType() {
		panic("Cannot compare values of different size and type")
	}
	return b.binaryEvaluate(ll, rr)
}

func (b BooleanExpr) binaryEvaluate(l, r datatypes.ColumnArray) datatypes.ColumnArray {
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.BooleanType)
	for i := 0; i < l.Size(); i++ {
		evalRes := b.evalFunc(l.GetValue(i), r.GetValue(i), l.GetType())
		builder.Append(evalRes)
	}
	return builder.Build()
}

var AndEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	return toBool(lData, arrowType) && toBool(rData, arrowType)
}

func NewAndExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"and", "AND", l, r, AndEvalFunc}}
}

var OrEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	return toBool(lData, arrowType) || toBool(rData, arrowType)
}

func NewOrExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"or", "OR", l, r, OrEvalFunc}}
}

func toBool(data interface{}, arrowType arrow.DataType) bool {
	switch arrowType {
	case datatypes.BooleanType:
		return data.(bool)
	case datatypes.Int8Type, datatypes.Int16Type, datatypes.Int32Type, datatypes.Int64Type,
		datatypes.UInt8Type, datatypes.UInt16Type, datatypes.UInt32Type, datatypes.UInt64Type:
		return data == 1
	default:
		panic(fmt.Sprintf("invalid toBool data: %v", data))
	}
}

var EqEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.BooleanType:
		return lData.(bool) == rData.(bool)
	case datatypes.Int8Type:
		return lData.(int8) == rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) == rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) == rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) == rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) == rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) == rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) == rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) == rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) == rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) == rData.(float64)
	case datatypes.StringType:
		return lData.(string) == rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewEqExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"eq", "=", l, r, EqEvalFunc}}
}

var NeqEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.BooleanType:
		return lData.(bool) != rData.(bool)
	case datatypes.Int8Type:
		return lData.(int8) != rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) != rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) != rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) != rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) != rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) != rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) != rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) != rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) != rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) != rData.(float64)
	case datatypes.StringType:
		return lData.(string) != rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewNeqExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"neq", "!=", l, r, NeqEvalFunc}}
}

var LtEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) < rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) < rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) < rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) < rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) < rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) < rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) < rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) < rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) < rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) < rData.(float64)
	case datatypes.StringType:
		return lData.(string) < rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewLtExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"lt", "<", l, r, LtEvalFunc}}
}

var LtEqEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) <= rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) <= rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) <= rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) <= rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) <= rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) <= rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) <= rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) <= rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) <= rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) <= rData.(float64)
	case datatypes.StringType:
		return lData.(string) <= rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewLtEqExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"lteq", "<=", l, r, LtEqEvalFunc}}
}

var GtEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) > rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) > rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) > rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) > rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) > rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) > rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) > rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) > rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) > rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) > rData.(float64)
	case datatypes.StringType:
		return lData.(string) > rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewGtExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"gt", ">", l, r, GtEvalFunc}}
}

var GtEqEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) >= rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) >= rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) >= rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) >= rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) >= rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) >= rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) >= rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) >= rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) >= rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) >= rData.(float64)
	case datatypes.StringType:
		return lData.(string) >= rData.(string)
	default:
		panic(fmt.Sprintf("boolEvaluate invliad type: %T", lData))
	}
}

func NewGtEqExpr(l, r physicalplan.PhysicalExpr) BooleanExpr {
	return BooleanExpr{BinaryExpr{"gteq", ">=", l, r, GtEqEvalFunc}}
}

// -----------------Math Expressions-----------------

type MathExpr struct {
	BinaryExpr
}

func (m MathExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	ll := m.l.Evaluate(input)
	rr := m.r.Evaluate(input)
	if ll.Size() != rr.Size() || ll.GetType() != rr.GetType() {
		panic("Cannot compare values of different size and type")
	}
	return m.binaryEvaluate(ll, rr)
}

func (m MathExpr) binaryEvaluate(l, r datatypes.ColumnArray) datatypes.ColumnArray {
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), l.GetType())
	for i := 0; i < l.Size(); i++ {
		evalRes := m.evalFunc(l.GetValue(i), r.GetValue(i), l.GetType())
		builder.Append(evalRes)
	}
	return builder.Build()
}

var AddEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) + rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) + rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) + rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) + rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) + rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) + rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) + rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) + rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) + rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) + rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewAddExpr(l, r physicalplan.PhysicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"add", "+", l, r, AddEvalFunc}}
}

var SubtractEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) - rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) - rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) - rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) - rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) - rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) - rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) - rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) - rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) - rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) - rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewSubtractExpr(l, r physicalplan.PhysicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"subtract", "-", l, r, SubtractEvalFunc}}
}

var MultiplyEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) * rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) * rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) * rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) * rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) * rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) * rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) * rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) * rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) * rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) * rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewMultiplyExpr(l, r physicalplan.PhysicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"multiply", "*", l, r, MultiplyEvalFunc}}
}

var DivideEvalFunc = func(lData, rData interface{}, arrowType arrow.DataType) interface{} {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) / rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) / rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) / rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) / rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) / rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) / rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) / rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) / rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) / rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) / rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewDivideExpr(l, r physicalplan.PhysicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"divide", "/", l, r, DivideEvalFunc}}
}
