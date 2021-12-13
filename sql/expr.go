package sql

import "fmt"

type Expr interface {
	toString() string
}

// Simple SQL identifier such as a table or column name
type Identifier struct {
	Id string
}

func (i Identifier) toString() string {
	return i.Id
}

// Binary expression
type BinaryExpr struct {
	L  Expr
	Op Expr
	R  Expr
}

func (b BinaryExpr) toString() string {
	return fmt.Sprintf("%s %s %s", b.L, b.Op, b.R)
}

// SQL literal string
type String struct {
	Value string
}

func (s String) toString() string {
	return s.Value
}

// SQL literal long
type Long struct {
	Value string
}

func (l Long) toString() string {
	return l.Value
}

// SQL literal double
type Double struct {
	Value string
}

func (d Double) toString() string {
	return d.Value
}

// SQL function call
type Function struct {
	Id   string
	Args []Expr
}

func (f Function) toString() string {
	return f.Id
}

// SQL aliased expression
type Alias struct {
	Expr  Expr
	Alias Identifier
}

func (a Alias) toString() string {
	return fmt.Sprintf("%s %s", a.Expr, a.Alias)
}

type Cast struct {
	Expr     Expr
	DataType Identifier
}

func (c Cast) toString() string {
	return fmt.Sprintf("%s %s", c.Expr, c.DataType)
}

type Sort struct {
	Expr Expr
	Asc  bool
}

func (s Sort) toString() string {
	return fmt.Sprintf("%s %t", s.Expr, s.Asc)
}

// SQL relation
type Select struct {
	Selection  Expr
	Projection []Expr
	TableName  string
	GroupBy    []Expr
	OrderBy    []Expr
	Having     Expr
}

func (s Select) toString() string {
	return fmt.Sprintf("%s %v %s %v %v %s", s.Selection, s.Projection, s.TableName, s.GroupBy, s.OrderBy, s.Having)
}
