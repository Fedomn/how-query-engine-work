package datasource

import (
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/stretchr/testify/require"
	"query-engine/datatypes"
	"testing"
)

var idData = []int8{int8(1), int8(2), int8(3), int8(4)}
var nameData = []string{"a", "b", "c", "d"}

func TestInMemDataSource_Schema(t *testing.T) {
	schema := buildSchema()
	data := buildRecordBatch(schema)

	memDS := NewInMemDataSource(schema, data)
	headers := []string{"Id", "Name"}
	for i, header := range headers {
		require.Equal(t, header, memDS.Schema().Fields[i].Name)
	}
}

func TestInMemDataSource_Scan_with_no_projection(t *testing.T) {
	schema := buildSchema()
	data := buildRecordBatch(schema)

	memDS := NewInMemDataSource(schema, data)
	cursor := 0
	for memDS.Next() {
		recordBatch := memDS.Scan([]string{})
		require.Equal(t, idData[cursor], recordBatch.Field(0).GetValue(0))
		require.Equal(t, nameData[cursor], recordBatch.Field(1).GetValue(0))
		cursor++
	}
}

func TestInMemDataSource_Scan_with_projection(t *testing.T) {
	schema := buildSchema()
	data := buildRecordBatch(schema)

	memDS := NewInMemDataSource(schema, data)
	cursor := 0
	for memDS.Next() {
		recordBatch := memDS.Scan([]string{"Name"})
		require.Equal(t, nameData[cursor], recordBatch.Field(0).GetValue(0))
		cursor++
	}
}

func buildSchema() datatypes.Schema {
	fields := []datatypes.Field{
		{"Id", datatypes.Int8Type},
		{"Name", datatypes.StringType},
	}
	schema := datatypes.Schema{Fields: fields}
	return schema
}

func buildRecordBatch(schema datatypes.Schema) datatypes.RecordBatch {
	idBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int8Type)
	nameBuilder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.StringType)
	idBuilder.AppendValues(int8(1), int8(2), int8(3), int8(4))
	nameBuilder.AppendValues("a", "b", "c", "d")

	return datatypes.RecordBatch{
		Schema: schema,
		Fields: []datatypes.ColumnArray{
			idBuilder.Build(),
			nameBuilder.Build(),
		},
	}
}
