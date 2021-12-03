package datasource

import (
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/types"
	"testing"
)

const filename = dir + "/alltypes_plain.parquet"

func TestParquetDataSource_Schema(t *testing.T) {
	pds := NewParquetDataSource(filename, 10)
	headers := []string{"Id", "Bool_col", "Tinyint_col", "Smallint_col", "Int_col", "Bigint_col", "Float_col", "Double_col", "Date_string_col", "String_col", "Timestamp_col"}
	for i, header := range headers {
		require.Equal(t, header, pds.Schema().Fields[i].Name)
	}
}

func TestParquetDataSource_Scan_with_no_projection(t *testing.T) {
	pds := NewParquetDataSource(filename, 5)
	result := pds.Scan([]string{})
	recordBatch := result[0]

	firstColumn := recordBatch.Field(0)
	firstColumnData := []int32{4, 5, 6, 7, 2}
	for i, datum := range firstColumnData {
		require.Equal(t, datum, firstColumn.GetValue(i))
	}
}

func TestParquetDataSource_Scan_with_projection(t *testing.T) {
	pds := NewParquetDataSource(filename, 2)
	result := pds.Scan([]string{"Timestamp_col"})
	recordBatch := result[0]

	firstColumn := recordBatch.Field(0)
	firstValue := firstColumn.GetValue(1)
	time := types.INT96ToTime(firstValue.(string))
	require.Equal(t, "2009-03-01 00:01:00 +0000 UTC", time.String())
}
