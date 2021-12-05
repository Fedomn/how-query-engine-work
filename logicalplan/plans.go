package logicalplan

import (
	"fmt"
	"query-engine/datasource"
	"query-engine/datatypes"
	"strings"
)

// Scan represents a scan of a DataSource with an optional projection.
// It is the only logical plan that doesn't have another logical plan as input.
// It is a leaf node in the query tree.
type Scan struct {
	path       string
	dataSource datasource.DataSource
	projection []string
}

func (s Scan) Schema() datatypes.Schema {
	schema := s.dataSource.Schema()
	pjSchema, _ := schema.SelectByName(s.projection)
	return pjSchema
}

func (s Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Scan) String() string {
	if len(s.projection) == 0 {
		return fmt.Sprintf("Scan: %s; projection=None", s.path)
	} else {
		return fmt.Sprintf("Scan: %s; projection=%v", s.path, s.projection)
	}
}

func NewScan(path string, datasource datasource.DataSource, projection []string) Scan {
	return Scan{path, datasource, projection}
}

// Projection apply a projection to its input logical plan.
// It is a list of exprs to be evaluated against the input data.
// Sometimes exprs are simple list of columns, complex example would be MathExpr, CastExpr and something else.
type Projection struct {
	input LogicalPlan
	exprs []LogicalExpr
}

func (p Projection) Schema() datatypes.Schema {
	fields := make([]datatypes.Field, len(p.exprs))
	for i, expr := range p.exprs {
		fields[i] = expr.ToField(p.input)
	}
	return datatypes.Schema{Fields: fields}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.input}
}

func (p Projection) String() string {
	exprStrList := make([]string, len(p.exprs))
	for i, expr := range p.exprs {
		exprStrList[i] = expr.String()
	}
	return fmt.Sprintf("Projection: %s", strings.Join(exprStrList, ", "))
}

func NewProjection(input LogicalPlan, exprs []LogicalExpr) Projection {
	return Projection{input, exprs}
}

// Selection apply a filter expression to determine which rows should be selected in its output.
// The filter expression needs to evaluate to a boolean result.
type Selection struct {
	input LogicalPlan
	expr  LogicalExpr
}

func (s Selection) Schema() datatypes.Schema {
	return s.input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Selection: %s", s.expr)
}

func NewSelection(input LogicalPlan, expr LogicalExpr) Selection {
	return Selection{input, expr}
}

// Aggregate calculates aggregates of underlying data.
type Aggregate struct {
	input     LogicalPlan
	groupExpr []LogicalExpr
	aggExpr   []AggregateExpr
}

func (a Aggregate) Schema() datatypes.Schema {
	fields := make([]datatypes.Field, len(a.groupExpr)+len(a.aggExpr))
	for i := 0; i < len(a.groupExpr); i++ {
		fields[i] = a.groupExpr[i].ToField(a.input)
	}
	for i := len(a.groupExpr); i < len(a.aggExpr); i++ {
		fields[i] = a.aggExpr[i].ToField(a.input)
	}
	return datatypes.Schema{Fields: fields}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%v, aggregateExpr=%v", a.groupExpr, a.aggExpr)
}

func NewAggregate(input LogicalPlan, groupExpr []LogicalExpr, aggExpr []AggregateExpr) Aggregate {
	return Aggregate{input, groupExpr, aggExpr}
}

type Limit struct {
	input LogicalPlan
	limit int
}

func (l Limit) Schema() datatypes.Schema {
	return l.input.Schema()
}

func (l Limit) Children() []LogicalPlan {
	return []LogicalPlan{l.input}
}

func (l Limit) String() string {
	return fmt.Sprintf("Limit: %d", l.limit)
}

func NewLimit(input LogicalPlan, limit int) Limit {
	return Limit{input, limit}
}
