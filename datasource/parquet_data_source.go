package datasource

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"query-engine/datatypes"
)

// ParquetDataSource only support non-nested table
type ParquetDataSource struct {
	filename  string
	schema    datatypes.Schema
	batchSize int
	pr        *reader.ParquetReader
	// current scan row pos
	cursor int64
	// the number of rows in recordBatch
	numRows int64

	// projection schema
	pjSchema datatypes.Schema
	// projection indices
	pjIndices []int
	// arrow array builders
	builders []datatypes.ArrowArrayBuilder
}

func NewParquetDataSource(filename string, batchSize int, projection []string) *ParquetDataSource {
	p := &ParquetDataSource{filename: filename, batchSize: batchSize}
	p.inferSchema(projection)
	return p
}

func (p *ParquetDataSource) Schema() datatypes.Schema {
	return p.schema
}

func (p *ParquetDataSource) Scan() datatypes.RecordBatch {
	for i, pjIdx := range p.pjIndices {
		data, _, _, err := p.pr.ReadColumnByIndex(int64(pjIdx), int64(p.batchSize))
		if err != nil {
			panic(fmt.Sprintf("parquet read data err: %v", err))
		}
		p.builders[i].AppendValues(data...)
	}

	p.cursor += int64(p.batchSize)
	fields := make([]datatypes.ColumnArray, len(p.pjIndices))
	for i := 0; i < len(p.builders); i++ {
		fields[i] = p.builders[i].Build()
	}
	return datatypes.RecordBatch{
		Schema: p.schema,
		Fields: fields,
	}
}

func (p *ParquetDataSource) Next() bool {
	return p.cursor < p.numRows
}

// parquet read refer to https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools/parquet-tools.go
func (p *ParquetDataSource) inferSchema(projection []string) {
	fr, err := local.NewLocalFileReader(p.filename)
	if err != nil {
		panic(fmt.Sprintf("Can't open file: %v", err))
	}
	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		panic(fmt.Sprintf("Can't create column reader: %v", err))
	}

	headers := make([]datatypes.Field, 0)
	elems := pr.SchemaHandler.SchemaElements
	for i := 1; i < len(elems); i++ {
		headers = append(headers, p.createFiled(elems[i]))
	}

	p.schema = datatypes.Schema{Fields: headers}
	p.pr = pr
	p.cursor = 0
	p.numRows = p.pr.GetNumRows()

	p.pjSchema, p.pjIndices = p.schema.SelectByName(projection)
	p.builders = make([]datatypes.ArrowArrayBuilder, len(p.pjSchema.Fields))
	for i, field := range p.pjSchema.Fields {
		p.builders[i] = datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType)
	}
}

func (p *ParquetDataSource) createFiled(elem *parquet.SchemaElement) datatypes.Field {
	var dType arrow.DataType
	switch elem.GetType() {
	case parquet.Type_BOOLEAN:
		dType = datatypes.BooleanType
	case parquet.Type_INT32:
		dType = datatypes.Int32Type
	case parquet.Type_INT64:
		dType = datatypes.Int64Type
	case parquet.Type_FLOAT:
		dType = datatypes.FloatType
	case parquet.Type_DOUBLE:
		dType = datatypes.DoubleType
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.Type_INT96:
		dType = datatypes.StringType
	default:
		panic(fmt.Sprintf("parquet not support type: %v", elem.GetType()))
	}

	return datatypes.Field{
		Name:     elem.GetName(),
		DataType: dType,
	}
}
