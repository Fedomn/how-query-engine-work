package logicalplan

import (
	"github.com/stretchr/testify/require"
	"query-engine/datasource"
	"query-engine/datatypes"
	"testing"
)

const dir = "../testdata"

func Test_BuildLogicalPlans(t *testing.T) {
	csv := datasource.NewCsvDataSource(dir+"/employee.csv", 1024, []string{})

	scan := NewScan("employee", csv, []string{})
	eq := NewEq(NewCol("state"), NewLiteralString("CO"))
	selection := NewSelection(scan, eq)

	plan := NewProjection(selection, []LogicalExpr{NewCol("id"), NewCol("first_name"), NewCol("last_name")})

	expect := `
Projection: #id, #first_name, #last_name
	Selection: #state = 'CO'
		Scan: employee; projection=None
`

	require.Equal(t, expect, PrettyFormat(plan))
}

func Test_BuildLogicalPlans_Aggregate(t *testing.T) {
	csv := datasource.NewCsvDataSource(dir+"/employee.csv", 1024, []string{})

	scan := NewScan("employee", csv, []string{})
	groupExpr := []LogicalExpr{NewCol("state")}
	aggExpr := []AggregateExpr{NewMax(NewCast(NewCol("salary"), datatypes.Int32Type))}
	plan := NewAggregate(scan, groupExpr, aggExpr)

	expect := `
Aggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS int32))]
	Scan: employee; projection=None
`

	require.Equal(t, expect, PrettyFormat(plan))
}
