package datatypes

import (
	"encoding/json"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func DetectType(data interface{}) string {
	d := reflect.ValueOf(data)
	switch d.Kind() {
	case reflect.Slice:
		return "Slice"
	case reflect.Map:
		return "Map"
	case reflect.Struct:
		return "Struct"
	case reflect.String:
		return "String"
	case reflect.Float64:
		return "Float64"
	}
	return ""
}

func DetectArrowType(data interface{}) arrow.DataType {
	d := reflect.ValueOf(data)
	switch d.Kind() {
	case reflect.String:
		return arrow.BinaryTypes.String
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64
	case reflect.Map:
		return arrow.StructOf(jsonToArrow(data, nil)...)
	}
	return nil
}

func jsonToArrow(data interface{}, fields []arrow.Field) []arrow.Field {
	if DetectType(data) == "Map" {
		for key, value := range data.(map[string]interface{}) {
			fields = append(fields, arrow.Field{
				Name:     key,
				Type:     DetectArrowType(value),
				Metadata: arrow.Metadata{},
			})
		}
	}
	return fields
}

func createArrowFields(data []interface{}) []arrow.Field {
	fields := make([]arrow.Field, len(data))
	for _, d := range data {
		fields = jsonToArrow(d, fields)
		return fields
	}
	return nil
}

func TestDynamicJson(t *testing.T) {
	jsonString := `
	[
	{
		"Name": "Adheip",
		"Age": 24,
		"Country": {
			"Code": 24
		},
		"City": {
			"Punjab": "India"
		}
	}
	]`

	var data []interface{}

	_ = json.Unmarshal([]byte(jsonString), &data)

	structFields := []arrow.Field{
		{
			Name: "Geek",
			Type: arrow.StructOf(createArrowFields(data)[1:]...),
		},
	}
	require.Equal(t, "Geek", structFields[0].Name)
	require.Equal(t, arrow.STRUCT, structFields[0].Type.ID())

	fields := []string{"Name", "Age", "Country", "City"}
	structType := structFields[0].Type.(*arrow.StructType)
	for i := range fields {
		require.Contains(t, fields, structType.Field(i).Name)
	}
}
