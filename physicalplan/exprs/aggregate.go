package exprs

import (
	"fmt"
	"query-engine/physicalplan"
)

// ---------------------------------------------Aggregate Expressions---------------------------------------------

type AggregateExpr interface {
	inputExpr() physicalplan.PhysicalExpr
	createAccumulator() Accumulator
}

type Accumulator interface {
	accumulate(val interface{})
	finalValue() interface{}
}

type SumExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewSumExpr(expr physicalplan.PhysicalExpr) SumExpr {
	return SumExpr{expr}
}

func (s SumExpr) inputExpr() physicalplan.PhysicalExpr {
	return s.expr
}

func (s SumExpr) createAccumulator() Accumulator {
	return &SumAccumulator{}
}

func (s SumExpr) String() string {
	return fmt.Sprintf("SUM(%s)", s.expr)
}

type SumAccumulator struct {
	val interface{}
}

func (s *SumAccumulator) accumulate(val interface{}) {
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

func (s *SumAccumulator) finalValue() interface{} {
	return s.val
}

type MaxExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewMaxExpr(expr physicalplan.PhysicalExpr) MaxExpr {
	return MaxExpr{expr}
}

func (m MaxExpr) inputExpr() physicalplan.PhysicalExpr {
	return m.expr
}

func (m MaxExpr) createAccumulator() Accumulator {
	return &MaxAccumulator{}
}

type MaxAccumulator struct {
	val interface{}
}

func (m *MaxAccumulator) accumulate(val interface{}) {
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

func (m *MaxAccumulator) finalValue() interface{} {
	return m.val
}

type MinExpr struct {
	expr physicalplan.PhysicalExpr
}

func NewMinExpr(expr physicalplan.PhysicalExpr) MinExpr {
	return MinExpr{expr}
}

func (m MinExpr) inputExpr() physicalplan.PhysicalExpr {
	return m.expr
}

func (m MinExpr) createAccumulator() Accumulator {
	return &MinAccumulator{}
}

type MinAccumulator struct {
	val interface{}
}

func (m *MinAccumulator) accumulate(val interface{}) {
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

func (m *MinAccumulator) finalValue() interface{} {
	return m.val
}
