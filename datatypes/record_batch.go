package datatypes

import (
	"fmt"
	"strings"
)

// RecordBatch : Batch of data organized in columns.
type RecordBatch struct {
	Schema Schema
	Fields []ColumnArray
}

func (r *RecordBatch) RowCount() int {
	// TODO
	return 0
}

func (r *RecordBatch) ColumnCount() int {
	return len(r.Fields)
}

func (r *RecordBatch) Field(i int) ColumnArray {
	return r.Fields[i]
}

// for better testing
func (r *RecordBatch) toCSV() string {
	b := make([]string, 0)
	for rowIdx := 0; rowIdx < r.RowCount(); rowIdx++ {
		for colIdx := 0; colIdx < r.ColumnCount(); colIdx++ {
			if colIdx > 0 {
				b = append(b, ",")
			}
			value := r.Fields[colIdx].GetValue(colIdx)
			if value == nil {
				b = append(b, "null")
			} else {
				b = append(b, fmt.Sprint(value))
			}
		}
		b = append(b, "\n")
	}
	return strings.Join(b, "")
}

func (r *RecordBatch) String() string {
	return r.toCSV()
}
