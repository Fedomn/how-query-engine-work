package datasource

import (
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/types"
	"testing"
)

const filename = dir + "/alltypes_plain.parquet"

func TestParquetDataSource_Schema(t *testing.T) {
	pds := NewParquetDataSource(filename, 10, []string{})
	headers := []string{"Id", "Bool_col", "Tinyint_col", "Smallint_col", "Int_col", "Bigint_col", "Float_col", "Double_col", "Date_string_col", "String_col", "Timestamp_col"}
	for i, header := range headers {
		require.Equal(t, header, pds.Schema().Fields[i].Name)
	}
}

func TestParquetDataSource_Scan_with_no_projection(t *testing.T) {
	pds := NewParquetDataSource(filename, 1204, []string{})
	recordBatch := pds.Scan()
	firstColumn := recordBatch.Field(0)
	firstColumnData := []int32{4, 5, 6, 7, 2, 3, 0, 1}
	for i, datum := range firstColumnData {
		require.Equal(t, datum, firstColumn.GetValue(i))
	}
}

func TestParquetDataSource_Scan_with_projection(t *testing.T) {
	pds := NewParquetDataSource(filename, 1024, []string{"Timestamp_col"})
	recordBatch := pds.Scan()
	firstColumn := recordBatch.Field(0)
	firstValue := firstColumn.GetValue(1)
	time := types.INT96ToTime(firstValue.(string))
	require.Equal(t, "2009-03-01 00:01:00 +0000 UTC", time.String())
}

func TestParquetDataSource_Scan_with_smallBatch(t *testing.T) {
	pds := NewParquetDataSource(filename, 3, []string{"Bool_col"})

	require.True(t, pds.Next())
	res := pds.Scan()
	require.Equal(t, 3, res.RowCount())

	require.True(t, pds.Next())
	res = pds.Scan()
	require.Equal(t, 3, res.RowCount())

	require.True(t, pds.Next())
	res = pds.Scan()
	require.Equal(t, 2, res.RowCount())
	require.False(t, pds.Next())

	firstColumn := res.Field(0)
	require.Equal(t, 2, firstColumn.Size())

	firstValue := firstColumn.GetValue(0)
	require.Equal(t, true, firstValue)
}
