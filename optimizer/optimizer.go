package optimizer

import (
	"fmt"
	. "query-engine/logicalplan"
)

type Optimizer struct {
	rules []Rule
}

func NewOptimizer() Optimizer {
	return Optimizer{rules: []Rule{ProjectionPushDownRule{}}}
}

func (o Optimizer) Optimize(plan LogicalPlan) LogicalPlan {
	iterPlan := plan
	for _, rule := range o.rules {
		iterPlan = rule.optimize(iterPlan)
	}
	return iterPlan
}

type Rule interface {
	optimize(plan LogicalPlan) LogicalPlan
}

type ProjectionPushDownRule struct{}

func (p ProjectionPushDownRule) optimize(plan LogicalPlan) LogicalPlan {
	accCols := make([]string, 0)
	return p.pushDown(plan, &accCols)
}

func (p ProjectionPushDownRule) pushDown(plan LogicalPlan, accCols *[]string) LogicalPlan {
	switch castPlan := plan.(type) {
	case Projection:
		p.extractColsForAllExpr(castPlan.Exprs, castPlan.Input, accCols)
		input := p.pushDown(castPlan.Input, accCols)
		return NewProjection(input, castPlan.Exprs)
	case Selection:
		p.extractCols(castPlan.Expr, castPlan.Input, accCols)
		input := p.pushDown(castPlan.Input, accCols)
		return NewSelection(input, castPlan.Expr)
	case Aggregate:
		p.extractColsForAllExpr(castPlan.GroupExpr, castPlan.Input, accCols)
		aggExprs := make([]LogicalExpr, 0)
		for _, ae := range castPlan.AggExpr {
			aggExprs = append(aggExprs, ae.Expr)
		}
		p.extractColsForAllExpr(aggExprs, castPlan.Input, accCols)
		input := p.pushDown(castPlan.Input, accCols)
		return NewAggregate(input, castPlan.GroupExpr, castPlan.AggExpr)
	case Scan:
		// the bottom logical plan
		return NewScan(castPlan.Path, castPlan.DataSource, p.distinctCols(*accCols))
	default:
		panic(fmt.Sprintf("ProjectionPushDownRule not support plan: %s", castPlan))
	}
}

func (p ProjectionPushDownRule) extractColsForAllExpr(expr []LogicalExpr, input LogicalPlan, accCols *[]string) {
	for _, e := range expr {
		p.extractCols(e, input, accCols)
	}
}

func (p ProjectionPushDownRule) extractCols(expr LogicalExpr, input LogicalPlan, accCols *[]string) {
	switch e := expr.(type) {
	case ColumnIndex:
		*accCols = append(*accCols, input.Schema().Fields[e.Index].Name)
	case Column:
		*accCols = append(*accCols, e.Name)
	case BooleanBinaryExpr:
		p.extractCols(e.L, input, accCols)
		p.extractCols(e.R, input, accCols)
	case MathExpr:
		p.extractCols(e.L, input, accCols)
		p.extractCols(e.R, input, accCols)
	case Alias:
		p.extractCols(e.Expr, input, accCols)
	case CastExpr:
		p.extractCols(e.Expr, input, accCols)
	case LiteralString, LiteralLong, LiteralFloat, LiteralDouble:
		// do nothing
	default:
		panic(fmt.Sprintf("extractCols not support expr: %s", e))
	}
}

func (p ProjectionPushDownRule) distinctCols(accCols []string) []string {
	colSet := make(map[string]struct{})
	newCols := make([]string, 0)
	for _, col := range accCols {
		if _, ok := colSet[col]; !ok {
			colSet[col] = struct{}{}
			newCols = append(newCols, col)
		}
	}
	return newCols
}
