package datatypes

import "github.com/apache/arrow/go/v6/arrow"

// Field represents a field
type Field struct {
	Name     string
	DataType arrow.DataType
}

func (f *Field) ToArrow() arrow.Field {
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

func (s *Schema) ToArrow() *arrow.Schema {
	fields := make([]arrow.Field, len(s.Fields))
	for idx, field := range s.Fields {
		fields[idx] = field.ToArrow()
	}
	return arrow.NewSchema(fields, nil)
}

func (s *Schema) ProjectByIdx(indices []int) Schema {
	fields := make([]Field, len(indices))
	for i, idx := range indices {
		fields[i] = s.Fields[idx]
	}
	return Schema{fields}
}

// SelectByName when names are empty, return the whole schema
func (s *Schema) SelectByName(names []string) (Schema, []int) {
	if len(names) == 0 {
		indices := make([]int, len(s.Fields))
		for i := 0; i < len(s.Fields); i++ {
			indices[i] = i
		}
		return *s, indices
	}

	fields := make([]Field, 0)
	indices := make([]int, 0)
	for _, name := range names {
		for idx, field := range s.Fields {
			if field.Name == name {
				fields = append(fields, field)
				indices = append(indices, idx)
			}
		}
	}
	return Schema{fields}, indices
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
