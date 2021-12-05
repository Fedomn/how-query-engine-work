package logicalplan

import (
	"github.com/stretchr/testify/require"
	"query-engine/datasource"
	"testing"
)

func csvDataFrame() DataFrame {
	csv := datasource.NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	return NewDefaultDataFrame(NewScan("employee", csv, []string{}))
}

func Test_BuildDataFrame(t *testing.T) {
	df := csvDataFrame()
	df = df.Filter(NewEq(NewCol("state"), NewLiteralString("CO")))
	df = df.Project([]LogicalExpr{NewCol("id"), NewCol("first_name"), NewCol("last_name")})

	expect := `
Projection: #id, #first_name, #last_name
	Selection: #state = 'CO'
		Scan: employee; projection=None
`
	require.Equal(t, expect, PrettyFormat(df.LogicalPlan()))
}

func Test_BuildDataFrame_multiplier_and_alias(t *testing.T) {
	df := csvDataFrame()
	df = df.Filter(NewEq(NewCol("state"), NewLiteralString("CO")))
	df = df.Project([]LogicalExpr{
		NewCol("id"),
		NewCol("first_name"),
		NewCol("last_name"),
		NewCol("salary"),
		NewAlias(NewMultiply(NewCol("salary"), NewLiteralDouble(0.1)), "bonus"),
	})
	df = df.Filter(NewGt(NewCol("bonus"), NewLiteralDouble(1000)))

	expect := `
Selection: #bonus > 1000
	Projection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus
		Selection: #state = 'CO'
			Scan: employee; projection=None
`
	require.Equal(t, expect, PrettyFormat(df.LogicalPlan()))
}

func Test_BuildDataFrame_Aggregate(t *testing.T) {
	df := csvDataFrame()
	df = df.Aggregate(
		[]LogicalExpr{
			NewCol("state"),
		},
		[]AggregateExpr{
			NewMin(NewCol("salary")),
			NewMax(NewCol("salary")),
			NewCount(NewCol("salary")),
		})

	expect := `
Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary) MAX(#salary) COUNT(#salary)]
	Scan: employee; projection=None
`
	require.Equal(t, expect, PrettyFormat(df.LogicalPlan()))
}
