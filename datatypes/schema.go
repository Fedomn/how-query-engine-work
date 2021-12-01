package datatypes

import "github.com/apache/arrow/go/v6/arrow"

// Field represents a field
type Field struct {
	Name     string
	DataType arrow.DataType
}

func (f *Field) toArrow() arrow.Field {
	return arrow.Field{
		Name:     f.Name,
		Type:     f.DataType,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

// Schema provides metadata for a datasource or the results from a query.
type Schema struct {
	Fields []Field
}

func (s *Schema) toArrow() *arrow.Schema {
	fields := make([]arrow.Field, len(s.Fields))
	for idx, field := range s.Fields {
		fields[idx] = field.toArrow()
	}
	return arrow.NewSchema(fields, nil)
}

func (s *Schema) projectByIdx(indices []int) Schema {
	fields := make([]Field, len(indices))
	for i, idx := range indices {
		fields[i] = s.Fields[idx]
	}
	return Schema{fields}
}

func (s *Schema) selectByName(names []string) Schema {
	fields := make([]Field, 0)
	for _, name := range names {
		for _, field := range s.Fields {
			if field.Name == name {
				fields = append(fields, field)
			}
		}
	}
	return Schema{fields}
}

func NewSchemaFromArrow(schema arrow.Schema) Schema {
	aFields := schema.Fields()
	fields := make([]Field, len(aFields))
	for idx, aField := range aFields {
		fields[idx] = Field{
			Name:     aField.Name,
			DataType: aField.Type,
		}
	}
	return Schema{fields}
}
