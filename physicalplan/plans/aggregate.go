package plans

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"query-engine/datatypes"
	"query-engine/physicalplan"
	"query-engine/physicalplan/exprs"
)

type HashAggregateExec struct {
	// input plan to scan datasource and produce recordBatch
	input physicalplan.PhysicalPlan

	// groupExpr to evaluate recordBatch and produce grouping keys. eg: Col.
	groupExpr []physicalplan.PhysicalExpr

	// aggExpr to evaluate recordBatch and produce needed agg exprs. eg: Col.
	// it will also produce needed accumulators for every aggExprs
	aggExpr []exprs.AggregateExpr

	// schema represents groupExprs and aggExprs
	schema datatypes.Schema

	// finished aggregation calculation
	done bool
}

func NewHashAggregateExec(input physicalplan.PhysicalPlan, groupExpr []physicalplan.PhysicalExpr, aggExpr []exprs.AggregateExpr, schema datatypes.Schema) HashAggregateExec {
	return HashAggregateExec{input, groupExpr, aggExpr, schema, false}
}

func (h HashAggregateExec) Schema() datatypes.Schema {
	return h.schema
}

func (h HashAggregateExec) Execute() datatypes.RecordBatch {
	row2AccMap := make(map[string][]exprs.Accumulator)
	rowGroupKeys := make([]string, 0)

	for h.input.Next() {
		recordBatch := h.input.Execute()
		// get needed grouping key columns
		groupKeyColumnArray := make([]datatypes.ColumnArray, len(h.groupExpr))
		for i, expr := range h.groupExpr {
			groupKeyColumnArray[i] = expr.Evaluate(recordBatch)
		}

		// get needed to be aggregated key columns
		aggKeyColumnArray := make([]datatypes.ColumnArray, len(h.aggExpr))
		for i, expr := range h.aggExpr {
			aggKeyColumnArray[i] = expr.InputExpr().Evaluate(recordBatch)
		}

		for rowIdx := 0; rowIdx < recordBatch.RowCount(); rowIdx++ {
			row := make([]interface{}, len(groupKeyColumnArray))
			for colIdx := 0; colIdx < len(groupKeyColumnArray); colIdx++ {
				row[colIdx] = groupKeyColumnArray[colIdx].GetValue(rowIdx)
			}

			groupKey := h.encodeCols(row)
			accs, ok := row2AccMap[groupKey]
			if !ok {
				rowGroupKeys = append(rowGroupKeys, groupKey)

				accumulators := make([]exprs.Accumulator, len(h.aggExpr))
				for i, expr := range h.aggExpr {
					accumulators[i] = expr.CreateAccumulator()
				}
				row2AccMap[groupKey] = accumulators
				accs = row2AccMap[groupKey]
			}

			// perform accumulation
			for i, acc := range accs {
				value := aggKeyColumnArray[i].GetValue(rowIdx)
				acc.Accumulate(value)
			}
		}
	}

	builders := make([]datatypes.ArrowArrayBuilder, 0)
	for _, field := range h.schema.Fields {
		builders = append(builders, datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType))
	}

	for _, rowGroupKey := range rowGroupKeys {
		cols := h.decodeCols(rowGroupKey)
		for i := 0; i < len(h.groupExpr); i++ {
			builders[i].Append(cols[i])
		}
		accumulators := row2AccMap[rowGroupKey]
		for i := 0; i < len(h.aggExpr); i++ {
			builders[i+len(h.groupExpr)].Append(accumulators[i].FinalValue())
		}
	}

	fields := make([]datatypes.ColumnArray, len(builders))
	for i := 0; i < len(fields); i++ {
		fields[i] = builders[i].Build()
	}
	// create result batch containing final aggregate values
	h.done = true
	return datatypes.RecordBatch{
		Schema: h.schema,
		Fields: fields,
	}
}

func (h HashAggregateExec) Next() bool {
	return !h.done
}

func (h HashAggregateExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{h.input}
}

func (h HashAggregateExec) String() string {
	return fmt.Sprintf("HashAggregateExec: groupExpr=%v, aggExpr=%v", h.groupExpr, h.aggExpr)
}

func (h HashAggregateExec) encodeCols(cols []interface{}) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cols)
	if err != nil {
		panic(fmt.Sprintf("HashAggregateExec encode cols err: %v", err))
	}
	return buf.String()
}

func (h HashAggregateExec) decodeCols(encStr string) []interface{} {
	var res []interface{}
	buf := bytes.NewBuffer([]byte(encStr))
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&res); err != nil {
		panic(fmt.Sprintf("HashAggregateExec decode err: %v", err))
	}
	return res
}
