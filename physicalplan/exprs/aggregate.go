package exprs

import (
	"fmt"
	"query-engine/physicalplan"
)

// ---------------------------------------------Aggregate Expressions---------------------------------------------

type AggregateExpr interface {
	InputExpr() physicalplan.PhysicalExpr
	CreateAccumulator() Accumulator
}

type Accumulator interface {
	Accumulate(val interface{})
	FinalValue() interface{}
}

type SumExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewSumExpr(expr physicalplan.PhysicalExpr) SumExpr {
	return SumExpr{expr}
}

func (s SumExpr) InputExpr() physicalplan.PhysicalExpr {
	return s.expr
}

func (s SumExpr) CreateAccumulator() Accumulator {
	return &SumAccumulator{}
}

func (s SumExpr) String() string {
	return fmt.Sprintf("SUM(%s)", s.expr)
}

type SumAccumulator struct {
	val interface{}
}

func (s *SumAccumulator) Accumulate(val interface{}) {
	if val != nil {
		if s.val == nil {
			s.val = val
		} else {
			switch v := s.val.(type) {
			case int8:
				s.val = v + val.(int8)
			case int16:
				s.val = v + val.(int16)
			case int32:
				s.val = v + val.(int32)
			case int64:
				s.val = v + val.(int64)
			case uint8:
				s.val = v + val.(uint8)
			case uint16:
				s.val = v + val.(uint16)
			case uint32:
				s.val = v + val.(uint32)
			case uint64:
				s.val = v + val.(uint64)
			case float32:
				s.val = v + val.(float32)
			case float64:
				s.val = v + val.(float64)
			default:
				panic(fmt.Sprintf("Sum is not implemented for type: %T", v))
			}
		}
	}
}

func (s *SumAccumulator) FinalValue() interface{} {
	return s.val
}

type MaxExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewMaxExpr(expr physicalplan.PhysicalExpr) MaxExpr {
	return MaxExpr{expr}
}

func (m MaxExpr) InputExpr() physicalplan.PhysicalExpr {
	return m.expr
}

func (m MaxExpr) CreateAccumulator() Accumulator {
	return &MaxAccumulator{}
}

func (m MaxExpr) String() string {
	return fmt.Sprintf("MAX(%s)", m.expr)
}

type MaxAccumulator struct {
	val interface{}
}

func (m *MaxAccumulator) Accumulate(val interface{}) {
	if val != nil {
		if m.val == nil {
			m.val = val
		} else {
			var isMax bool
			switch v := m.val.(type) {
			case int8:
				isMax = val.(int8) > v
			case int16:
				isMax = val.(int16) > v
			case int32:
				isMax = val.(int32) > v
			case int64:
				isMax = val.(int64) > v
			case uint8:
				isMax = val.(uint8) > v
			case uint16:
				isMax = val.(uint16) > v
			case uint32:
				isMax = val.(uint32) > v
			case uint64:
				isMax = val.(uint64) > v
			case float32:
				isMax = val.(float32) > v
			case float64:
				isMax = val.(float64) > v
			default:
				panic(fmt.Sprintf("Max is not implemented for type: %T", v))
			}
			if isMax {
				m.val = val
			}
		}
	}
}

func (m *MaxAccumulator) FinalValue() interface{} {
	return m.val
}

type MinExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewMinExpr(expr physicalplan.PhysicalExpr) MinExpr {
	return MinExpr{expr}
}

func (m MinExpr) InputExpr() physicalplan.PhysicalExpr {
	return m.expr
}

func (m MinExpr) CreateAccumulator() Accumulator {
	return &MinAccumulator{}
}

func (m MinExpr) String() string {
	return fmt.Sprintf("MIN(%s)", m.expr)
}

type MinAccumulator struct {
	val interface{}
}

func (m *MinAccumulator) Accumulate(val interface{}) {
	if val != nil {
		if m.val == nil {
			m.val = val
		} else {
			var isMin bool
			switch v := m.val.(type) {
			case int8:
				isMin = val.(int8) < v
			case int16:
				isMin = val.(int16) < v
			case int32:
				isMin = val.(int32) < v
			case int64:
				isMin = val.(int64) < v
			case uint8:
				isMin = val.(uint8) < v
			case uint16:
				isMin = val.(uint16) < v
			case uint32:
				isMin = val.(uint32) < v
			case uint64:
				isMin = val.(uint64) < v
			case float32:
				isMin = val.(float32) < v
			case float64:
				isMin = val.(float64) < v
			default:
				panic(fmt.Sprintf("Min is not implemented for type: %T", v))
			}
			if isMin {
				m.val = val
			}
		}
	}
}

func (m *MinAccumulator) FinalValue() interface{} {
	return m.val
}
