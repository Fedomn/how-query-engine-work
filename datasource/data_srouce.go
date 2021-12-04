package datasource

import "query-engine/datatypes"

type DataSource interface {
	// Schema Return the schema for the underlying data source
	Schema() datatypes.Schema

	// Scan the data source, selecting the specified columns
	Scan() datatypes.RecordBatch

	// Next prepares the next recordBatch for reading with then Scan method
	Next() bool
}
