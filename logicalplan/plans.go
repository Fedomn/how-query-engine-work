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
	Path       string
	DataSource datasource.DataSource
	Projection []string
}

func (s Scan) Schema() datatypes.Schema {
	schema := s.DataSource.Schema()
	pjSchema, _ := schema.SelectByName(s.Projection)
	return pjSchema
}

func (s Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Scan) String() string {
	if len(s.Projection) == 0 {
		return fmt.Sprintf("Scan: %s; projection=None", s.Path)
	} else {
		return fmt.Sprintf("Scan: %s; projection=%v", s.Path, s.Projection)
	}
}

func NewScan(path string, datasource datasource.DataSource, projection []string) Scan {
	return Scan{path, datasource, projection}
}

// Projection apply a projection to its input logical plan.
// It is a list of exprs to be evaluated against the input data.
// Sometimes exprs are simple list of columns, complex example would be MathExpr, CastExpr and something else.
type Projection struct {
	Input LogicalPlan
	Exprs []LogicalExpr
}

func (p Projection) Schema() datatypes.Schema {
	fields := make([]datatypes.Field, len(p.Exprs))
	for i, expr := range p.Exprs {
		fields[i] = expr.ToField(p.Input)
	}
	return datatypes.Schema{Fields: fields}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p Projection) String() string {
	exprStrList := make([]string, len(p.Exprs))
	for i, expr := range p.Exprs {
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
	Input LogicalPlan
	Expr  LogicalExpr
}

func (s Selection) Schema() datatypes.Schema {
	return s.Input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Selection: %s", s.Expr)
}

func NewSelection(input LogicalPlan, expr LogicalExpr) Selection {
	return Selection{input, expr}
}

// Aggregate calculates aggregates of underlying data.
type Aggregate struct {
	Input     LogicalPlan
	GroupExpr []LogicalExpr
	AggExpr   []AggregateExpr
}

func (a Aggregate) Schema() datatypes.Schema {
	fields := make([]datatypes.Field, len(a.GroupExpr)+len(a.AggExpr))
	for i := 0; i < len(a.GroupExpr); i++ {
		fields[i] = a.GroupExpr[i].ToField(a.Input)
	}
	for i := len(a.GroupExpr); i < len(a.AggExpr); i++ {
		fields[i] = a.AggExpr[i].ToField(a.Input)
	}
	return datatypes.Schema{Fields: fields}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%v, aggregateExpr=%v", a.GroupExpr, a.AggExpr)
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
