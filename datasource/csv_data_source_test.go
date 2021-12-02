package datasource

import (
	"github.com/stretchr/testify/require"
	"testing"
)

const dir = "./testdata"

func TestCsvDataSource_Scan_with_no_projection(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024)
	csv.Schema()
	result := csv.Scan([]string{})

	require.Len(t, result, 1)

	recordBatch := result[0]
	require.Len(t, recordBatch.Fields, 6)

	headers := []string{"id", "first_name", "last_name", "state", "job_title", "salary"}
	for i, header := range headers {
		require.Equal(t, header, recordBatch.Schema.Fields[i].Name)
	}

	firstField := recordBatch.Field(0)
	require.Equal(t, 4, firstField.Size(), "a column field should has 4 items")

	firstFieldData := []string{"1", "2", "3", "4"}
	for i, datum := range firstFieldData {
		require.Equal(t, datum, firstField.GetValue(i))
	}
}

func TestCsvDataSource_Scan_with_projection(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 1024)
	csv.Schema()
	result := csv.Scan([]string{"id", "state", "salary"})

	require.Len(t, result, 1)

	recordBatch := result[0]
	require.Len(t, recordBatch.Fields, 3)

	headers := []string{"id", "state", "salary"}
	for i, header := range headers {
		require.Equal(t, header, recordBatch.Schema.Fields[i].Name)
	}

	secondField := recordBatch.Field(1)
	require.Equal(t, 4, secondField.Size(), "a column field should has 4 items")

	secondFieldData := []string{"CA", "CO", "CO", ""}
	for i, datum := range secondFieldData {
		require.Equal(t, datum, secondField.GetValue(i))
	}
}

func TestCsvDataSource_Scan_with_smallBatch(t *testing.T) {
	csv := NewCsvDataSource(dir+"/employee.csv", 2)
	csv.Schema()
	result := csv.Scan([]string{"state", "salary"})

	require.Len(t, result, 2)

	recordBatch := result[1]
	require.Len(t, recordBatch.Fields, 2)

	headers := []string{"state", "salary"}
	for i, header := range headers {
		require.Equal(t, header, recordBatch.Schema.Fields[i].Name)
	}

	secondField := recordBatch.Field(1)
	require.Equal(t, 2, secondField.Size(), "a column field should has 2 items")

	secondFieldData := []string{"11500", "11500"}
	for i, datum := range secondFieldData {
		require.Equal(t, datum, secondField.GetValue(i))
	}
}
