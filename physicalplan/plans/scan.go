package plans

import (
	"fmt"
	"query-engine/datasource"
	"query-engine/datatypes"
	"query-engine/physicalplan"
)

type ScanExec struct {
	ds         datasource.DataSource
	projection []string
}

func (s ScanExec) Schema() datatypes.Schema {
	d := s.ds.Schema()
	schema, _ := d.SelectByName(s.projection)
	return schema
}

func (s ScanExec) Execute() datatypes.RecordBatch {
	return s.ds.Scan(s.projection)
}

func (s ScanExec) Next() bool {
	return s.ds.Next()
}

func (s ScanExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{}
}

func (s ScanExec) String() string {
	return fmt.Sprintf("ScanExec: schema=%s, projection=%v", s.Schema(), s.projection)
}

func NewScanExec(ds datasource.DataSource, projection []string) ScanExec {
	return ScanExec{ds, projection}
}
