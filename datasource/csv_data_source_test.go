package datasource

import (
	"github.com/stretchr/testify/require"
	"testing"
)

const dir = "../testdata"

func TestCsvDataSource_Schema(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	csv.Next()
	recordBatch := csv.Scan()

	require.Len(t, recordBatch.Fields, 6)

	headers := []string{"id", "first_name", "last_name", "state", "job_title", "salary"}
	for i, header := range headers {
		require.Equal(t, header, recordBatch.Schema.Fields[i].Name)
	}
}

func TestCsvDataSource_Scan_with_no_projection(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{})
	csv.Next()
	recordBatch := csv.Scan()

	require.Len(t, recordBatch.Fields, 6)

	firstColumn := recordBatch.Field(0)
	require.Equal(t, 4, firstColumn.Size(), "a column field should has 4 items")

	firstFieldData := []string{"1", "2", "3", "4"}
	for i, datum := range firstFieldData {
		require.Equal(t, datum, firstColumn.GetValue(i))
	}
}

func TestCsvDataSource_Scan_with_projection(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024, []string{"id", "state", "salary"})
	csv.Next()
	recordBatch := csv.Scan()
	require.Len(t, recordBatch.Fields, 3)

	secondColumn := recordBatch.Field(1)
	require.Equal(t, 4, secondColumn.Size(), "a column field should has 4 items")

	secondFieldData := []string{"CA", "CO", "CO", ""}
	for i, datum := range secondFieldData {
		require.Equal(t, datum, secondColumn.GetValue(i))
	}
}

func TestCsvDataSource_Scan_with_smallBatch(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 2, []string{"state", "salary"})

	csv.Next()
	recordBatch := csv.Scan()
	secondField := recordBatch.Field(1)
	secondFieldData := []string{"12000", "10000"}
	for i, datum := range secondFieldData {
		require.Equal(t, datum, secondField.GetValue(i))
	}

	csv.Next()
	recordBatch = csv.Scan()
	firstField := recordBatch.Field(0)
	firstFieldData := []string{"CO", ""}
	for i, datum := range firstFieldData {
		require.Equal(t, datum, firstField.GetValue(i))
	}
}
