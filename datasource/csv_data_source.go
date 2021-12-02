package datasource

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"io"
	"io/ioutil"
	"query-engine/datatypes"
)

// CsvDataSource must have headers
type CsvDataSource struct {
	filename  string
	schema    datatypes.Schema
	batchSize int
	csvReader *csv.Reader
}

func NewCsvDataSource(filename string, batchSize int) *CsvDataSource {
	ds := &CsvDataSource{
		filename:  filename,
		batchSize: batchSize,
	}
	ds.inferSchema()
	return ds
}

func (c *CsvDataSource) Schema() datatypes.Schema {
	return c.schema
}

func (c *CsvDataSource) Scan(projection []string) []datatypes.RecordBatch {
	readSchema, readIndices := c.schema.SelectByName(projection)
	res := make([]datatypes.RecordBatch, 0)

	batchBuf := make([][]string, 0, c.batchSize)
	iterCnt := 0
	for {
		// read one row from csv, then createBatch into columnar memory format
		record, err := c.csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("csv read err: %v", err))
		}
		batchBuf = append(batchBuf, record)
		iterCnt++
		if iterCnt == c.batchSize {
			res = append(res, c.createBatch(readSchema, readIndices, batchBuf))
			iterCnt = 0
			batchBuf = batchBuf[:0]
		}
	}
	if len(batchBuf) != 0 {
		res = append(res, c.createBatch(readSchema, readIndices, batchBuf))
	}
	return res
}

func (c *CsvDataSource) inferSchema() {
	file, err := ioutil.ReadFile(c.filename)
	if err != nil {
		panic(fmt.Sprintf("csv file: %s not exist!", c.filename))
	}

	r := csv.NewReader(bytes.NewReader(file))
	firstRecord, err := r.Read()
	if err != nil {
		panic(fmt.Sprintf("csv read err: %v", err))
	}

	headers := make([]datatypes.Field, 0)
	for _, record := range firstRecord {
		headers = append(headers, datatypes.Field{
			Name:     record,
			DataType: &datatypes.StringType{},
		})
	}

	c.schema = datatypes.Schema{Fields: headers}
	c.csvReader = r
}

func (c *CsvDataSource) createBatch(
	readSchema datatypes.Schema, readIndices []int, batchBuf [][]string,
) datatypes.RecordBatch {
	builders := make([]datatypes.ArrowArrayBuilder, 0)
	for _, field := range readSchema.Fields {
		builders = append(builders, datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType))
	}

	for i := 0; i < len(batchBuf); i++ {
		row := batchBuf[i]
		for j := 0; j < len(readIndices); j++ {
			builders[j].Append(row[readIndices[j]])
		}
	}

	fields := make([]datatypes.ColumnArray, len(readSchema.Fields))
	for i := 0; i < len(builders); i++ {
		fields[i] = builders[i].Build()
	}
	return datatypes.RecordBatch{
		Schema: readSchema,
		Fields: fields,
	}
}
