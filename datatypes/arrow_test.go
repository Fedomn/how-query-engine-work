package datatypes

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"testing"
)

func TestArrowBasic(t *testing.T) {
	// allocate memory
	pool := memory.NewGoAllocator()

	// define data type, data type are primitive and nested
	// struct is a nested data type.
	// struct type holds
	// 1. fields
	// 2. index
	// 3. metadata
	// Fields consist of
	// 1. Name
	// 2. DataType
	// 3. Null
	// 4. Metadata

	dtype := arrow.StructOf([]arrow.Field{
		{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
	}...)

	// New Struct Builder consists of :
	// 1. Data types
	// 2. Fields
	// 3  Builder
	// builder provides common functionality for managing the validity bitmap (nulls) when building arrays.

	sb := array.NewStructBuilder(pool, dtype)
	// Release is removing memory, with reference count to 0, mem freed.
	defer sb.Release()

	// field one builder
	// type cast to list builder
	f1b := sb.FieldBuilder(0).(*array.ListBuilder)
	defer f1b.Release()

	// field one value builder
	// type cast to Unsigned int8
	f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
	defer f1vb.Release()

	// field 2 builder, pass i as 1 to field,
	// type cast to int32 builder
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)
	defer f2b.Release()

	// there are 4 structs array
	// [{‘joe’, 1}, {null, 2}, null, {‘mark’, 4}]
	// reserve 4
	sb.Reserve(4)

	// characters field one has total 7 characters
	// 1. Joe -- 3
	// 2. Null --
	// 3. Null --
	// 4. Mark -- 4
	// Total: 7
	f1vb.Reserve(7)

	// field 2 has total 3 int32
	// 1, 2 and 4
	f2b.Reserve(3)

	// Append to form {'joe', 1}
	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("joe"), nil)
	f2b.Append(1)

	// Append to form {'null', 2}
	sb.Append(true)
	f1b.AppendNull()
	f2b.Append(2)

	// Append to form null
	sb.AppendNull()

	// Append to form {'mark', 4}
	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("mark"), nil)
	f2b.Append(4)

	// new array
	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	fmt.Printf("NullN() = %d\n", arr.NullN())
	fmt.Printf("Len()   = %d\n", arr.Len())

	// field 0 is a list
	list := arr.Field(0).(*array.List)
	defer list.Release()

	// [0 3 3 3 7]
	// joe has 3 chars + nul + nul + 4
	// 3 . 3 . 3 . 7 (add +4 )
	offsets := list.Offsets()
	fmt.Printf("%d\n", offsets)

	varr := list.ListValues().(*array.Uint8)
	defer varr.Release()

	ints := arr.Field(1).(*array.Int32)
	defer ints.Release()

	for i := 0; i < arr.Len(); i++ {
		if !arr.IsValid(i) {
			fmt.Printf("Struct[%d] = (null)\n", i)
			continue
		}
		fmt.Printf("Struct[%d] = [", i)
		pos := int(offsets[i])
		switch {
		case list.IsValid(pos):
			fmt.Printf("[")
			for j := offsets[i]; j < offsets[i+1]; j++ {
				if j != offsets[i] {
					fmt.Printf(", ")
				}
				fmt.Printf("%v", string(varr.Value(int(j))))
			}
			fmt.Printf("], ")
		default:
			fmt.Printf("(null), ")
		}
		fmt.Printf("%d]\n", ints.Value(i))
	}
}

func TestArrowJson(t *testing.T) {
	/*
		type Geek struct {
			Name string
			Age Float64
			Country struct {
				Code string
				City []string
			}
		}
		{
			"Name": "Adheip",
			"Age": 24
			"Country": {
				"Code": "IND",
				"City": [
					"Chandigarh",
					"Banglore",
				]
			}
		}
	*/

	pool := memory.NewGoAllocator()

	fields := []arrow.Field{
		{Name: "Geek", Type: arrow.StructOf(
			[]arrow.Field{
				{Name: "Name", Type: arrow.BinaryTypes.String},
				{Name: "Age", Type: arrow.PrimitiveTypes.Float64},
				{Name: "Country", Type: arrow.StructOf(
					[]arrow.Field{
						{Name: "Code", Type: arrow.BinaryTypes.String},
						{Name: "City", Type: arrow.ListOf(arrow.BinaryTypes.String)},
					}...),
				},
			}...)},
	}

	schema := arrow.NewSchema(fields, nil)

	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	sb := bld.Field(0).(*array.StructBuilder)
	defer sb.Release()

	// name
	f1b := sb.FieldBuilder(0).(*array.StringBuilder)
	defer f1b.Release()

	// age
	f2b := sb.FieldBuilder(1).(*array.Float64Builder)
	defer f2b.Release()

	// country
	f3b := sb.FieldBuilder(2).(*array.StructBuilder)
	defer f3b.Release()

	// country.code (field 3 -> internal field 1)
	f3F1b := f3b.FieldBuilder(0).(*array.StringBuilder)
	defer f3F1b.Release()

	// country.city
	f3F2b := f3b.FieldBuilder(1).(*array.ListBuilder)
	defer f3F2b.Release()
	f3F2vb := f3F2b.ValueBuilder().(*array.StringBuilder)
	defer f3F2vb.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Adheip"}, nil)
	f2b.AppendValues([]float64{24}, nil)
	f3b.AppendValues([]bool{true})
	f3F1b.AppendValues([]string{"IND"}, nil)
	f3F2b.Append(true)
	f3F2vb.AppendValues([]string{"Chandigarh", "Banglore"}, nil)

	rec1 := bld.NewRecord()
	defer rec1.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Nitish"}, nil)
	f2b.AppendValues([]float64{32}, nil)
	f3b.AppendValues([]bool{true})
	f3F1b.AppendValues([]string{"IND"}, nil)
	f3F2b.Append(true)
	f3F2vb.AppendValues([]string{"Ranchi", "Banglore"}, nil)

	rec2 := bld.NewRecord()
	defer rec2.Release()

	tbl := array.NewTableFromRecords(schema, []array.Record{rec1, rec2})
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 5)
	defer tr.Release()

	n := 0
	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
}
