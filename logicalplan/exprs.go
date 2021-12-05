package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"query-engine/datatypes"
)

// ---------------------------------------------Column Expressions---------------------------------------------

// Column Logical expression representing a reference to a column by name.
type Column struct {
	name string
}

func (c Column) ToField(input LogicalPlan) datatypes.Field {
	for _, field := range input.Schema().Fields {
		if field.Name == c.name {
			return field
		}
	}
	panic(fmt.Sprintf("No column named %s", c.name))
}

func (c Column) String() string {
	return fmt.Sprintf("#%s", c.name)
}

func NewCol(name string) Column {
	return Column{name}
}

// ColumnIndex Logical expression representing a reference to a column by index.
type ColumnIndex struct {
	index int
}

func (c ColumnIndex) ToField(input LogicalPlan) datatypes.Field {
	return input.Schema().Fields[c.index]
}

func (c ColumnIndex) String() string {
	return fmt.Sprintf("#%d", c.index)
}

func NewColumnIndex(i int) ColumnIndex {
	return ColumnIndex{i}
}

// ---------------------------------------------Literal Expressions---------------------------------------------

// LiteralString Logical expression representing a literal string value.
type LiteralString struct {
	str string
}

func (l LiteralString) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     l.str,
		DataType: datatypes.StringType,
	}
}

func (l LiteralString) String() string {
	return fmt.Sprintf("'%s'", l.str)
}

func NewLiteralString(str string) LiteralString {
	return LiteralString{str}
}

// LiteralLong Logical expression representing a literal long value.
type LiteralLong struct {
	n int64
}

func (l LiteralLong) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     fmt.Sprintf("%d", l.n),
		DataType: datatypes.Int64Type,
	}
}

func (l LiteralLong) String() string {
	return fmt.Sprintf("%d", l.n)
}

func NewLiteralLong(n int64) LiteralLong {
	return LiteralLong{n}
}

// LiteralFloat Logical expression representing a literal float value.
type LiteralFloat struct {
	n float32
}

func (l LiteralFloat) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     fmt.Sprintf("%g", l.n),
		DataType: datatypes.FloatType,
	}
}

func (l LiteralFloat) String() string {
	return fmt.Sprintf("%g", l.n)
}

// LiteralDouble Logical expression representing a literal double value.
type LiteralDouble struct {
	n float64
}

func (l LiteralDouble) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     fmt.Sprintf("%g", l.n),
		DataType: datatypes.DoubleType,
	}
}

func (l LiteralDouble) String() string {
	return fmt.Sprintf("%g", l.n)
}

// ---------------------------------------------Cast Expressions---------------------------------------------

// CastExpr Cast current logical expr type to target dataType
type CastExpr struct {
	expr  LogicalExpr
	dType arrow.DataType
}

func (c CastExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     c.expr.ToField(input).Name,
		DataType: c.dType,
	}
}

func (c CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", c.expr, c.dType)
}

func NewCast(expr LogicalExpr, dType arrow.DataType) CastExpr {
	return CastExpr{expr, dType}
}

// ---------------------------------------------Binary Expressions---------------------------------------------

// BinaryExpr an expression take two inputs, like comparison, boolean and math expressions.
type BinaryExpr struct {
	name string
	op   string
	l    LogicalExpr
	r    LogicalExpr
}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.l, b.op, b.r)
}

// BooleanBinaryExpr an expression return a boolean type
type BooleanBinaryExpr struct {
	BinaryExpr
}

func (b BooleanBinaryExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     b.name,
		DataType: datatypes.BooleanType,
	}
}

func NewAnd(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"and", "AND", l, r}}
}
func NewOr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"or", "OR", l, r}}
}
func NewEq(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"eq", "=", l, r}}
}
func NewNeq(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"neq", "!=", l, r}}
}
func NewGt(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"gt", ">", l, r}}
}
func NewGtEq(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"gteq", ">=", l, r}}
}
func NewLt(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"lt", "<", l, r}}
}
func NewLtEq(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"lteq", "<=", l, r}}
}

// MathExpr an expression return an input datatype
type MathExpr struct {
	BinaryExpr
}

func (m MathExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     m.name,
		DataType: m.l.ToField(input).DataType,
	}
}

func NewAdd(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"add", "+", l, r}}
}
func NewSubtract(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"subtract", "-", l, r}}
}
func NewMultiply(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"multiply", "*", l, r}}
}
func NewDivide(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"divide", "/", l, r}}
}
func NewModulus(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"modulus", "%", l, r}}
}

// ---------------------------------------------Unary Expressions---------------------------------------------

// UnaryExpr operate on one expr
type UnaryExpr struct {
	name string
	op   string
	expr LogicalExpr
}

func (u UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", u.op, u.expr)
}

type Not struct {
	UnaryExpr
}

func (n Not) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{
		Name:     n.name,
		DataType: datatypes.BooleanType,
	}
}

func NewNot(expr LogicalExpr) Not {
	return Not{UnaryExpr{"not", "NOT", expr}}
}

// ---------------------------------------------Alias Expressions---------------------------------------------

// Alias expression aliased
type Alias struct {
	expr  LogicalExpr
	alias string
}

func (a Alias) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{Name: a.alias, DataType: a.expr.ToField(input).DataType}
}

func (a Alias) String() string {
	return fmt.Sprintf("%s as %s", a.expr, a.alias)
}

// ---------------------------------------------Scalar Functions---------------------------------------------

type ScalarFunction struct {
	name       string
	args       []LogicalExpr
	returnType arrow.DataType
}

func (s ScalarFunction) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{Name: s.name, DataType: s.returnType}
}

func (s ScalarFunction) String() string {
	return fmt.Sprintf("%s(%v)", s.name, s.args)
}

// ---------------------------------------------Aggregate Expressions---------------------------------------------

type AggregateExpr struct {
	name string
	expr LogicalExpr
}

func (a AggregateExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{Name: a.name, DataType: a.expr.ToField(input).DataType}
}

func (a AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", a.name, a.expr)
}

func NewSum(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"SUM", input}
}
func NewMin(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MIN", input}
}
func NewMax(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MAX", input}
}
func NewAvg(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"AVG", input}
}

type AggregateCountExpr struct {
	expr LogicalExpr
}

func (a AggregateCountExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{Name: "COUNT", DataType: datatypes.UInt64Type}
}

func (a AggregateCountExpr) String() string {
	return fmt.Sprintf("COUNT(%s)", a.expr)
}

func NewCount(input LogicalExpr) AggregateCountExpr {
	return AggregateCountExpr{input}
}

type AggregateCountDistinctExpr struct {
	expr LogicalExpr
}

func (a AggregateCountDistinctExpr) ToField(input LogicalPlan) datatypes.Field {
	return datatypes.Field{Name: "COUNT_DISTINCT", DataType: datatypes.UInt64Type}
}

func (a AggregateCountDistinctExpr) String() string {
	return fmt.Sprintf("COUNT(DISTINCT %s)", a.expr)
}

func NewCountDistinct(input LogicalExpr) AggregateCountDistinctExpr {
	return AggregateCountDistinctExpr{input}
}
