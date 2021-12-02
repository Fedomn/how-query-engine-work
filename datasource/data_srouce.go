package datasource

import "query-engine/datatypes"

type DataSource interface {
	// Schema Return the schema for the underlying data source
	Schema() datatypes.Schema

	// Scan the data source, selecting the specified columns
	Scan(projection []string) []datatypes.RecordBatch
}
