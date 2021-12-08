package plans

import (
	"bytes"
	"encoding/gob"
	"github.com/stretchr/testify/require"
	"query-engine/datasource"
	"query-engine/datatypes"
	"query-engine/physicalplan"
	"query-engine/physicalplan/exprs"
	"testing"
)

func TestHashAggregateExec_encode_decode(t *testing.T) {
	input := []interface{}{true}

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(input); err != nil {
		t.Error(err)
	}

	var res []interface{}
	b := bytes.NewBuffer([]byte(buf.String()))
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&res); err != nil {
		t.Error(err)
	}

	for i, key := range res {
		require.Equal(t, input[i], key)
	}
}

func TestHashAggregateExec(t *testing.T) {
	pds := datasource.NewParquetDataSource(filename, 10, []string{})
	headers := []string{"Id", "Bool_col", "Tinyint_col", "Smallint_col", "Int_col", "Bigint_col", "Float_col", "Double_col", "Date_string_col", "String_col", "Timestamp_col"}
	for i, header := range headers {
		require.Equal(t, header, pds.Schema().Fields[i].Name)
	}

	scan := NewScanExec(pds, []string{})
	groupExpr := []physicalplan.PhysicalExpr{exprs.NewColumnExpr(1)}
	aggExpr := []exprs.AggregateExpr{exprs.NewMaxExpr(exprs.NewColumnExpr(0)), exprs.NewMinExpr(exprs.NewColumnExpr(0))}
	schema := datatypes.Schema{Fields: []datatypes.Field{
		{"Bool_col", datatypes.BooleanType},
		{"Max(Id)", datatypes.Int32Type},
		{"Min(Id)", datatypes.Int32Type},
	}}

	plan := NewHashAggregateExec(scan, groupExpr, aggExpr, schema)

	result := plan.Execute()
	require.Equal(t, 2, result.RowCount())

	field0 := []bool{true, false}
	field1 := []int32{6, 7}
	field2 := []int32{0, 1}
	for i := 0; i < 2; i++ {
		require.Equal(t, field0[i], result.Field(0).GetValue(i))
		require.Equal(t, field1[i], result.Field(1).GetValue(i))
		require.Equal(t, field2[i], result.Field(2).GetValue(i))
	}

	planFormat := `
HashAggregateExec: groupExpr=[#1], aggExpr=[SUM(#0) MIN(#0)]
	ScanExec: schema={[{Id int32} {Bool_col bool} {Tinyint_col int32} {Smallint_col int32} {Int_col int32} {Bigint_col int64} {Float_col float32} {Double_col float64} {Date_string_col utf8} {String_col utf8} {Timestamp_col utf8}]}, projection=[]
`
	require.Equal(t, planFormat, physicalplan.PrettyFormat(plan))
}
