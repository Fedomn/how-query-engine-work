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
}

func NewParquetDataSource(filename string, batchSize int) *ParquetDataSource {
	p := &ParquetDataSource{filename: filename, batchSize: batchSize}
	p.inferSchema()
	return p
}

func (p *ParquetDataSource) Schema() datatypes.Schema {
	return p.schema
}

func (p *ParquetDataSource) Scan(projection []string) []datatypes.RecordBatch {
	readSchema, readIndices := p.schema.SelectByName(projection)
	res := make([]datatypes.RecordBatch, 0)
	builders := make([]datatypes.ArrowArrayBuilder, 0)
	for _, field := range readSchema.Fields {
		builders = append(builders, datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType))
	}

	readCnt := 0
	rowCnt := p.pr.GetNumRows()
	for readCnt < int(rowCnt) {
		for i := 0; i < len(readIndices); i++ {
			data, _, _, err := p.pr.ReadColumnByIndex(int64(readIndices[i]), int64(p.batchSize))
			if err != nil {
				panic(fmt.Sprintf("parquet read data err: %v", err))
			}
			for _, datum := range data {
				builders[i].Append(datum)
			}
		}

		fields := make([]datatypes.ColumnArray, len(readSchema.Fields))
		for i := 0; i < len(builders); i++ {
			fields[i] = builders[i].Build()
		}
		res = append(res, datatypes.RecordBatch{Schema: readSchema, Fields: fields})
		readCnt += p.batchSize
	}
	return res
}

// parquet read refer to https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools/parquet-tools.go
func (p *ParquetDataSource) inferSchema() {
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
}

func (p *ParquetDataSource) createFiled(elem *parquet.SchemaElement) datatypes.Field {
	var dType arrow.DataType
	switch elem.GetType() {
	case parquet.Type_BOOLEAN:
		dType = &datatypes.BooleanType{}
	case parquet.Type_INT32:
		dType = &datatypes.Int32Type{}
	case parquet.Type_INT64:
		dType = &datatypes.Int64Type{}
	case parquet.Type_FLOAT:
		dType = &datatypes.FloatType{}
	case parquet.Type_DOUBLE:
		dType = &datatypes.DoubleType{}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.Type_INT96:
		dType = &datatypes.StringType{}
	default:
		panic(fmt.Sprintf("parquet not support type: %v", elem.GetType()))
	}

	return datatypes.Field{
		Name:     elem.GetName(),
		DataType: dType,
	}
}
