package exprs

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"query-engine/datatypes"
	"query-engine/physicalplan"
	"strconv"
)

// ---------------------------------------------Cast Expressions---------------------------------------------

type Cast struct {
	expr  physicalplan.PhysicalExpr
	dType arrow.DataType
}

func (c Cast) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", c.expr, c.dType)
}

func (c Cast) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	columnArray := c.expr.Evaluate(input)
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), c.dType)

	switch c.dType {
	case datatypes.Int8Type:
		for i := 0; i < columnArray.Size(); i++ {
			v := columnArray.GetValue(i)
			if v == nil {
				builder.Append(nil)
			} else {
				var castValue int8
				switch vv := v.(type) {
				case int, int16, int32, int64:
					castValue = v.(int8)
				case string:
					castValue = int8(c.strToInt64(vv))
				default:
					panic(fmt.Sprintf("Cannot cast value:%v to int8", vv))
				}
				builder.Append(castValue)
			}
		}
	case datatypes.Int64Type:
		for i := 0; i < columnArray.Size(); i++ {
			v := columnArray.GetValue(i)
			if v == nil {
				builder.Append(nil)
			} else {
				var castValue int64
				switch vv := v.(type) {
				case int, int8, int16, int32:
					castValue = v.(int64)
				case string:
					castValue = int64(c.strToInt64(vv))
				default:
					panic(fmt.Sprintf("Cannot cast value:%v to int64", vv))
				}
				builder.Append(castValue)
			}
		}
	case datatypes.FloatType:
		for i := 0; i < columnArray.Size(); i++ {
			v := columnArray.GetValue(i)
			if v == nil {
				builder.Append(nil)
			} else {
				var castValue float32
				switch vv := v.(type) {
				case int, int8, int16, int32:
					castValue = v.(float32)
				case string:
					castValue = c.strToFloat32(vv)
				default:
					panic(fmt.Sprintf("Cannot cast value:%v to float", vv))
				}
				builder.Append(castValue)
			}
		}
	case datatypes.DoubleType:
		for i := 0; i < columnArray.Size(); i++ {
			v := columnArray.GetValue(i)
			if v == nil {
				builder.Append(nil)
			} else {
				var castValue float64
				switch vv := v.(type) {
				case int, int8, int16, int32:
					castValue = v.(float64)
				case string:
					castValue = c.strToFloat64(vv)
				default:
					panic(fmt.Sprintf("Cannot cast value:%v to double", vv))
				}
				builder.Append(castValue)
			}
		}
	case datatypes.StringType:
		for i := 0; i < columnArray.Size(); i++ {
			v := columnArray.GetValue(i)
			if v == nil {
				builder.Append(nil)
			} else {
				builder.Append(fmt.Sprint(v))
			}
		}
	default:
		panic(fmt.Sprintf("Unsupport type:%v cast", c.dType))
	}

	return builder.Build()
}

func (c Cast) strToInt64(v string) int {
	vv, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("Cast %s to int err: %v", v, err))
	}
	return vv
}

func (c Cast) strToFloat32(v string) float32 {
	vv, err := strconv.ParseFloat(v, 32)
	if err != nil {
		panic(fmt.Sprintf("Cast %s to float32 err: %v", v, err))
	}
	return float32(vv)
}

func (c Cast) strToFloat64(v string) float64 {
	vv, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(fmt.Sprintf("Cast %s to float64 err: %v", v, err))
	}
	return vv
}

func NewCastExpr(expr physicalplan.PhysicalExpr, dType arrow.DataType) Cast {
	return Cast{expr, dType}
}
