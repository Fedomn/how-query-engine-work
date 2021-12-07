package plans

import (
	"fmt"
	"query-engine/datatypes"
	"query-engine/physicalplan"
)

type ProjectionExec struct {
	// already selected RecordBatch plan
	input physicalplan.PhysicalPlan
	// used for the following exprs
	schema datatypes.Schema
	// projections exprs, like Column, ColumnIndex...
	exprs []physicalplan.PhysicalExpr
}

func (p ProjectionExec) Schema() datatypes.Schema {
	return p.schema
}

func (p ProjectionExec) Execute() datatypes.RecordBatch {
	recordBatch := p.input.Execute()
	fields := make([]datatypes.ColumnArray, len(p.exprs))
	for i := 0; i < len(p.exprs); i++ {
		fields[i] = p.exprs[i].Evaluate(recordBatch)
	}
	return datatypes.RecordBatch{Schema: p.schema, Fields: fields}
}

func (p ProjectionExec) Next() bool {
	return p.input.Next()
}

func (p ProjectionExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{p.input}
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %v", p.exprs)
}

func NewProjectionExec(input physicalplan.PhysicalPlan, schema datatypes.Schema, exprs []physicalplan.PhysicalExpr) ProjectionExec {
	return ProjectionExec{input, schema, exprs}
}
