package execution

import (
	"query-engine/datasource"
	"query-engine/datatypes"
	. "query-engine/logicalplan"
	"query-engine/optimizer"
	"query-engine/physicalplan"
	"query-engine/queryplaner"
)

type Ctx struct {
	BatchSize    int
	PhysicalPlan physicalplan.PhysicalPlan
}

func NewCtx() *Ctx {
	return &Ctx{BatchSize: 1024}
}

func (c *Ctx) CSV(filename string) DataFrame {
	csvDataSource := datasource.NewCsvDataSource(filename, c.BatchSize)
	scan := NewScan(filename, csvDataSource, []string{})
	return NewDefaultDataFrame(scan)
}

func (c *Ctx) Parquet(filename string) DataFrame {
	parquetDataSource := datasource.NewParquetDataSource(filename, c.BatchSize)
	scan := NewScan(filename, parquetDataSource, []string{})
	return NewDefaultDataFrame(scan)
}

func (c *Ctx) Plan(plan LogicalPlan) {
	optimizedPlan := optimizer.NewOptimizer().Optimize(plan)
	c.PhysicalPlan = queryplaner.NewPhysicalPlan(optimizedPlan)
}

func (c *Ctx) Next() bool {
	return c.PhysicalPlan.Next()
}

func (c *Ctx) Execute() datatypes.RecordBatch {
	return c.PhysicalPlan.Execute()
}
