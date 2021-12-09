package optimizer

import (
	"github.com/stretchr/testify/require"
	. "query-engine/datasource"
	"query-engine/datatypes"
	. "query-engine/logicalplan"
	"testing"
)

const dir = "../testdata"

func TestOptimizer_pushDown(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	scan := NewScan("employee", csv, []string{})
	plan := NewProjection(scan, []LogicalExpr{NewCol("id"), NewCol("first_name"), NewCol("last_name")})

	beforePlan := `
Projection: #id, #first_name, #last_name
	Scan: employee; projection=None
`
	require.Equal(t, beforePlan, PrettyFormat(plan))

	afterPlan := `
Projection: #id, #first_name, #last_name
	Scan: employee; projection=[id first_name last_name]
`
	optimizedPlan := NewOptimizer().Optimize(plan)
	require.Equal(t, afterPlan, PrettyFormat(optimizedPlan))
}

func TestOptimizer_pushDown_with_selection(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	scan := NewScan("employee", csv, []string{})
	eq := NewEq(NewCol("state"), NewLiteralString("CO"))
	selection := NewSelection(scan, eq)
	plan := NewProjection(selection, []LogicalExpr{NewCol("id"), NewCol("first_name"), NewCol("last_name")})

	beforePlan := `
Projection: #id, #first_name, #last_name
	Selection: #state = 'CO'
		Scan: employee; projection=None
`
	require.Equal(t, beforePlan, PrettyFormat(plan))

	afterPlan := `
Projection: #id, #first_name, #last_name
	Selection: #state = 'CO'
		Scan: employee; projection=[id first_name last_name state]
`
	optimizedPlan := NewOptimizer().Optimize(plan)
	require.Equal(t, afterPlan, PrettyFormat(optimizedPlan))
}

func TestOptimizer_pushDown_with_aggregate(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	scan := NewScan("employee", csv, []string{})
	groupExpr := []LogicalExpr{NewCol("state")}
	aggExpr := []AggregateExpr{NewMax(NewCast(NewCol("salary"), datatypes.Int32Type))}
	plan := NewAggregate(scan, groupExpr, aggExpr)

	beforePlan := `
Aggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS int32))]
	Scan: employee; projection=None
`
	require.Equal(t, beforePlan, PrettyFormat(plan))

	afterPlan := `
Aggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS int32))]
	Scan: employee; projection=[state salary]
`
	optimizedPlan := NewOptimizer().Optimize(plan)
	require.Equal(t, afterPlan, PrettyFormat(optimizedPlan))
}
