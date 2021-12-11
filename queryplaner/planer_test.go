package queryplaner

import (
	"github.com/stretchr/testify/require"
	"query-engine/datasource"
	. "query-engine/logicalplan"
	"query-engine/optimizer"
	"query-engine/physicalplan"
	"testing"
)

const dir = "../testdata"

func csvDataFrame() DataFrame {
	csv := datasource.NewCsvDataSource(dir+"/employee.csv", 1024)
	return NewDefaultDataFrame(NewScan("employee", csv, []string{}))
}

func TestAggregatePlan(t *testing.T) {
	df := csvDataFrame()
	df = df.Aggregate(
		[]LogicalExpr{
			NewCol("state"),
		},
		[]AggregateExpr{
			NewMin(NewCol("salary")),
			NewMax(NewCol("salary")),
			NewSum(NewCol("salary")),
		})
	expect := `
Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary) MAX(#salary) SUM(#salary)]
	Scan: employee; projection=None
`
	require.Equal(t, expect, PrettyFormat(df.LogicalPlan()))

	optimizedPlan := optimizer.NewOptimizer().Optimize(df.LogicalPlan())
	expect = `
Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary) MAX(#salary) SUM(#salary)]
	Scan: employee; projection=[state salary]
`
	require.Equal(t, expect, PrettyFormat(optimizedPlan))

	plan := NewPhysicalPlan(optimizedPlan)
	expect = `
HashAggregateExec: groupExpr=[#0], aggExpr=[MIN(#1) SUM(#1) SUM(#1)]
	ScanExec: schema={[{state utf8} {salary utf8}]}, projection=[state salary]
`
	require.Equal(t, expect, physicalplan.PrettyFormat(plan))
}
