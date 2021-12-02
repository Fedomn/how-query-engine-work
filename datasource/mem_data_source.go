package datasource

import "query-engine/datatypes"

type InMemDataSource struct {
	schema datatypes.Schema
	// all data are consist of multiple RecordBatches
	data []datatypes.RecordBatch
}

func NewInMemDataSource(schema datatypes.Schema, data []datatypes.RecordBatch) *InMemDataSource {
	return &InMemDataSource{
		schema: schema,
		data:   data,
	}
}

func (memDS *InMemDataSource) Schema() datatypes.Schema {
	return memDS.schema
}

func (memDS *InMemDataSource) Scan(projection []string) []datatypes.RecordBatch {
	pjIndices := make([]int, 0)
	for _, pj := range projection {
		for idx, field := range memDS.schema.Fields {
			if field.Name == pj {
				pjIndices = append(pjIndices, idx)
			}
		}
	}

	batches := make([]datatypes.RecordBatch, 0)

	for _, datum := range memDS.data {
		fields := make([]datatypes.ColumnArray, len(pjIndices))
		for i := 0; i < len(pjIndices); i++ {
			fields[i] = datum.Field(i)
		}
		batches = append(batches, datatypes.RecordBatch{
			Schema: memDS.schema,
			Fields: fields,
		})
	}

	return batches
}
