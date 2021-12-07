package plans

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"query-engine/datatypes"
	"query-engine/physicalplan"
)

type SelectionExec struct {
	// children plan: scan
	input physicalplan.PhysicalPlan

	// filter expressions for input. example: Eq, Or and other BooleanBinaryExpr.
	// these exprs always used in SQL where condition.
	expr physicalplan.PhysicalExpr
}

func (s SelectionExec) Schema() datatypes.Schema {
	return s.input.Schema()
}

func (s SelectionExec) Execute() datatypes.RecordBatch {
	recordBatch := s.input.Execute()
	// use boolean binary expr to evaluate input recordBatch
	// and produce a bool columnArray to represents current row whether matched or not
	rowBoolEvalResult := s.expr.Evaluate(recordBatch)
	if rowBoolEvalResult.GetType() != datatypes.BooleanType {
		panic(fmt.Sprintf("SelectionExec unexpect expr: %s and type: %s", s.expr, rowBoolEvalResult.GetType()))
	}

	filteredColumnArray := s.filter(recordBatch, rowBoolEvalResult)
	return datatypes.RecordBatch{Schema: recordBatch.Schema, Fields: filteredColumnArray}
}

func (s SelectionExec) Next() bool {
	return s.input.Next()
}

func (s SelectionExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{s.input}
}

func (s SelectionExec) String() string {
	return fmt.Sprintf("Selection: %s", s.expr)
}

func (s SelectionExec) filter(recordBatch datatypes.RecordBatch, rowBoolEvalResult datatypes.ColumnArray) []datatypes.ColumnArray {
	fields := make([]datatypes.ColumnArray, recordBatch.ColumnCount())
	for i := 0; i < recordBatch.ColumnCount(); i++ {
		columnArray := recordBatch.Field(i)
		b := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), columnArray.GetType())
		for j := 0; j < rowBoolEvalResult.Size(); j++ {
			if rowBoolEvalResult.GetValue(j).(bool) {
				b.Append(columnArray.GetValue(j))
			}
		}
		fields[i] = b.Build()
	}
	return fields
}

func NewSelectionExec(input physicalplan.PhysicalPlan, expr physicalplan.PhysicalExpr) SelectionExec {
	return SelectionExec{input, expr}
}
