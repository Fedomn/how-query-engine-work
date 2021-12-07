package plans

import (
	"github.com/stretchr/testify/require"
	"query-engine/datasource"
	"query-engine/datatypes"
	"query-engine/physicalplan"
	"query-engine/physicalplan/exprs"
	"testing"
)

const dir = "../../testdata"
const filename = dir + "/alltypes_plain.parquet"

func TestPhysicalPlan_Scan(t *testing.T) {
	pds := datasource.NewParquetDataSource(filename, 10, []string{})
	headers := []string{"Id", "Bool_col", "Tinyint_col", "Smallint_col", "Int_col", "Bigint_col", "Float_col", "Double_col", "Date_string_col", "String_col", "Timestamp_col"}
	for i, header := range headers {
		require.Equal(t, header, pds.Schema().Fields[i].Name)
	}

	scan := NewScanExec(pds, []string{})
	eq1 := exprs.NewEqExpr(exprs.NewColumnExpr(5), exprs.NewLiteralLongExpr(10))
	eq2 := exprs.NewEqExpr(exprs.NewColumnExpr(8), exprs.NewLiteralStringExpr("01/01/09"))
	and := exprs.NewAndExpr(eq1, eq2)
	selection := NewSelectionExec(scan, and)

	fields := []datatypes.Field{
		{"Id", datatypes.Int32Type},
		{"Bool_col", datatypes.BooleanType},
		{"Smallint_col", datatypes.Int32Type},
	}
	schema := datatypes.Schema{Fields: fields}
	physicalExprs := []physicalplan.PhysicalExpr{exprs.NewColumnExpr(0), exprs.NewColumnExpr(1), exprs.NewColumnExpr(3)}
	plan := NewProjectionExec(selection, schema, physicalExprs)

	require.True(t, plan.Next())
	result := plan.Execute()
	require.Equal(t, 1, result.RowCount())

	expectCol0MatchedIds := []int32{1}
	for i, id := range expectCol0MatchedIds {
		require.Equal(t, id, result.Fields[0].GetValue(i))
	}

	require.False(t, plan.Next())

	planFormat := `
ProjectionExec: [#0 #1 #3]
	Selection: #5 = 10 AND #8 = '01/01/09'
		ScanExec: schema={[{Id int32} {Bool_col bool} {Tinyint_col int32} {Smallint_col int32} {Int_col int32} {Bigint_col int64} {Float_col float32} {Double_col float64} {Date_string_col utf8} {String_col utf8} {Timestamp_col utf8}]}, projection=[]
`
	require.Equal(t, planFormat, physicalplan.PrettyFormat(plan))
}
