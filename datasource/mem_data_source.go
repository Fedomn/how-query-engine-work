package datasource

import (
	"github.com/apache/arrow/go/v6/arrow/memory"
	"query-engine/datatypes"
)

// InMemDataSource batchScanSize is 1
type InMemDataSource struct {
	schema datatypes.Schema
	// all data are organized in one RecordBatch that contains multiple columns
	data datatypes.RecordBatch
	// current scan row pos
	cursor int
	// the number of rows in recordBatch
	numRows int

	// projection schema
	pjSchema datatypes.Schema
	// projection indices
	pjIndices []int
	// arrow array builders
	builders []datatypes.ArrowArrayBuilder
}

func NewInMemDataSource(schema datatypes.Schema, data datatypes.RecordBatch, projection []string) *InMemDataSource {
	readSchema, readIndices := schema.SelectByName(projection)
	builders := make([]datatypes.ArrowArrayBuilder, len(readSchema.Fields))
	for i, field := range readSchema.Fields {
		builders[i] = datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType)
	}
	memDS := &InMemDataSource{
		schema:    schema,
		data:      data,
		cursor:    0,
		numRows:   data.RowCount(),
		pjSchema:  readSchema,
		pjIndices: readIndices,
		builders:  builders,
	}
	return memDS
}

func (memDS *InMemDataSource) Schema() datatypes.Schema {
	return memDS.schema
}

func (memDS *InMemDataSource) Scan() datatypes.RecordBatch {
	for i, pjIdx := range memDS.pjIndices {
		colDatum := memDS.data.Fields[pjIdx]
		memDS.builders[i].Append(colDatum.GetValue(memDS.cursor))
	}
	memDS.cursor++

	fields := make([]datatypes.ColumnArray, len(memDS.pjIndices))
	for i := 0; i < len(memDS.pjIndices); i++ {
		fields[i] = memDS.builders[i].Build()
	}
	return datatypes.RecordBatch{
		Schema: memDS.schema,
		Fields: fields,
	}
}

func (memDS *InMemDataSource) Next() bool {
	return memDS.cursor < memDS.numRows
}
