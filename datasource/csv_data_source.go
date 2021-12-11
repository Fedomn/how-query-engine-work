package datasource

import (
	"bytes"
	rawCsv "encoding/csv"
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/csv"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"io/ioutil"
	"query-engine/datatypes"
	"sync"
)

// CsvDataSource must have headers
type CsvDataSource struct {
	filename  string
	schema    datatypes.Schema
	batchSize int
	csvReader *csv.Reader

	// due to csv reader can't get total line nums, so use next() method to hold recordBatch
	cursorBatchBuf [][]string

	// projection schema
	pjSchema datatypes.Schema
	// projection indices
	pjIndices []int
	// arrow array builders
	builders []datatypes.ArrowArrayBuilder

	once sync.Once
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

func (c *CsvDataSource) Scan(projection []string) datatypes.RecordBatch {
	c.once.Do(func() {
		c.inferProjection(projection)
	})

	rec := c.csvReader.Record()

	fields := make([]datatypes.ColumnArray, len(c.pjSchema.Fields))
	for i := 0; i < len(c.pjIndices); i++ {
		data := array.NewStringData(rec.Column(c.pjIndices[i]).Data())
		fields[i] = datatypes.NewArrowFieldArray(data)
	}
	return datatypes.RecordBatch{
		Schema: c.pjSchema,
		Fields: fields,
	}
}

func (c *CsvDataSource) Next() bool {
	return c.csvReader.Next()
}

func (c *CsvDataSource) inferSchema() {
	file, err := ioutil.ReadFile(c.filename)
	if err != nil {
		panic(fmt.Sprintf("csv file: %s not exist!", c.filename))
	}

	r := rawCsv.NewReader(bytes.NewReader(file))
	firstRecord, err := r.Read()
	if err != nil {
		panic(fmt.Sprintf("csv read err: %v", err))
	}

	headers := make([]datatypes.Field, 0)
	for _, record := range firstRecord {
		headers = append(headers, datatypes.Field{
			Name:     record,
			DataType: datatypes.StringType,
		})
	}

	c.schema = datatypes.Schema{Fields: headers}
	c.csvReader = csv.NewReader(bytes.NewReader(file), c.schema.ToArrow(), csv.WithChunk(c.batchSize), csv.WithHeader(true))

	c.builders = make([]datatypes.ArrowArrayBuilder, len(c.schema.Fields))
	for i, field := range c.schema.Fields {
		c.builders[i] = datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.DataType)
	}
}

func (c *CsvDataSource) inferProjection(projection []string) {
	c.pjSchema, c.pjIndices = c.schema.SelectByName(projection)
}

func (c *CsvDataSource) createBatch(
	readSchema datatypes.Schema, readIndices []int, batchBuf [][]string,
) datatypes.RecordBatch {
	for i := 0; i < len(batchBuf); i++ {
		row := batchBuf[i]
		for j := 0; j < len(readIndices); j++ {
			c.builders[j].Append(row[readIndices[j]])
		}
	}

	fields := make([]datatypes.ColumnArray, len(readSchema.Fields))
	for i := 0; i < len(readIndices); i++ {
		fields[i] = c.builders[i].Build()
	}
	return datatypes.RecordBatch{
		Schema: readSchema,
		Fields: fields,
	}
}
