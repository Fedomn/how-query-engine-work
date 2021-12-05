package execution

import (
	"query-engine/datasource"
	. "query-engine/logicalplan"
)

type Ctx struct {
	batchSize int
}

func NewCtx() Ctx {
	return Ctx{batchSize: 1024}
}

func (c Ctx) CSV(filename string) DataFrame {
	csvDataSource := datasource.NewCsvDataSource(filename, c.batchSize, []string{})
	scan := NewScan(filename, csvDataSource, []string{})
	return NewDefaultDataFrame(scan)
}

func (c Ctx) Parquet(filename string) DataFrame {
	parquetDataSource := datasource.NewParquetDataSource(filename, c.batchSize, []string{})
	scan := NewScan(filename, parquetDataSource, []string{})
	return NewDefaultDataFrame(scan)
}
