package queryplaner

import (
	"fmt"
	"query-engine/datatypes"
	"query-engine/logicalplan"
	"query-engine/physicalplan"
	"query-engine/physicalplan/exprs"
	"query-engine/physicalplan/plans"
)

func NewPhysicalPlan(plan logicalplan.LogicalPlan) physicalplan.PhysicalPlan {
	switch p := plan.(type) {
	case logicalplan.Scan:
		return plans.NewScanExec(p.DataSource, p.Projection)
	case logicalplan.Selection:
		return plans.NewSelectionExec(NewPhysicalPlan(p.Input), NewPhysicalExpr(p.Expr, p.Input))
	case logicalplan.Projection:
		input := NewPhysicalPlan(p.Input)
		physicalExprs := make([]physicalplan.PhysicalExpr, len(p.Exprs))
		fields := make([]datatypes.Field, len(p.Exprs))
		for i, expr := range p.Exprs {
			physicalExprs[i] = NewPhysicalExpr(expr, p.Input)
			fields[i] = expr.ToField(p.Input)
		}
		schema := datatypes.Schema{Fields: fields}
		return plans.NewProjectionExec(input, schema, physicalExprs)
	case logicalplan.Aggregate:
		input := NewPhysicalPlan(p.Input)

		groupExprs := make([]physicalplan.PhysicalExpr, len(p.GroupExpr))
		for i, expr := range p.GroupExpr {
			groupExprs[i] = NewPhysicalExpr(expr, p.Input)
		}

		aggExprs := make([]exprs.AggregateExpr, len(p.AggExpr))
		for i, expr := range p.AggExpr {
			switch expr.Name {
			case "SUM":
				aggExprs[i] = exprs.NewSumExpr(NewPhysicalExpr(expr.Expr, p.Input))
			case "MIN":
				aggExprs[i] = exprs.NewMinExpr(NewPhysicalExpr(expr.Expr, p.Input))
			case "MAX":
				aggExprs[i] = exprs.NewMaxExpr(NewPhysicalExpr(expr.Expr, p.Input))
			default:
				panic(fmt.Sprintf("Unsupported aggregate function: %s", expr.Name))
			}
		}
		return plans.NewHashAggregateExec(input, groupExprs, aggExprs, p.Schema())
	default:
		panic(fmt.Sprintf("Unsupported plan: %s", p))
	}
}

func NewPhysicalExpr(expr logicalplan.LogicalExpr, input logicalplan.LogicalPlan) physicalplan.PhysicalExpr {
	switch e := expr.(type) {
	case logicalplan.LiteralLong:
		return exprs.NewLiteralLongExpr(e.N)
	case logicalplan.LiteralDouble:
		return exprs.NewLiteralDoubleExpr(e.N)
	case logicalplan.LiteralString:
		return exprs.NewLiteralStringExpr(e.Str)
	case logicalplan.ColumnIndex:
		return exprs.NewColumnIndexExpr(e.Index)
	case logicalplan.Alias:
		// note that there is no physical expression for an alias since the alias
		// only affects the name using in the planning phase and not how the aliased
		// expression is executed
		return NewPhysicalExpr(e.Expr, input)
	case logicalplan.Column:
		schema := input.Schema()
		idx := schema.FindFirstIndexByName(e.Name)
		if idx < 0 {
			panic(fmt.Sprintf("No column named: %s", e.Name))
		}
		return exprs.NewColumnIndexExpr(idx)
	case logicalplan.CastExpr:
		return exprs.NewCastExpr(NewPhysicalExpr(e.Expr, input), e.DType)
	case logicalplan.BooleanBinaryExpr:
		l := NewPhysicalExpr(e.L, input)
		r := NewPhysicalExpr(e.R, input)
		switch e.Name {
		case "eq":
			return exprs.NewEqExpr(l, r)
		case "neq":
			return exprs.NewNeqExpr(l, r)
		case "gt":
			return exprs.NewGtExpr(l, r)
		case "gteq":
			return exprs.NewGtEqExpr(l, r)
		case "lt":
			return exprs.NewLtExpr(l, r)
		case "lteq":
			return exprs.NewLtEqExpr(l, r)
		case "and":
			return exprs.NewAndExpr(l, r)
		case "or":
			return exprs.NewOrExpr(l, r)
		case "add":
			return exprs.NewAddExpr(l, r)
		case "subtract":
			return exprs.NewSubtractExpr(l, r)
		case "multiply":
			return exprs.NewMultiplyExpr(l, r)
		case "divide":
			return exprs.NewDivideExpr(l, r)
		default:
			panic(fmt.Sprintf("Unsupported binary expression: %s", e))
		}
	default:
		panic(fmt.Sprintf("Unsupported logical expression: %s", e))
	}
}
