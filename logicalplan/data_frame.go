package logicalplan

import "query-engine/datatypes"

type DataFrame interface {
	Project(expr []LogicalExpr) DataFrame
	Filter(expr LogicalExpr) DataFrame
	Aggregate(groupBy []LogicalExpr, aggExpr []AggregateExpr) DataFrame

	// Schema Returns the schema of the data that will be produced by this DataFrame.
	Schema() datatypes.Schema

	// LogicalPlan Get the logical plan.
	LogicalPlan() LogicalPlan
}

type DefaultDataFrame struct {
	plan LogicalPlan
}

func (d DefaultDataFrame) Project(expr []LogicalExpr) DataFrame {
	return DefaultDataFrame{NewProjection(d.plan, expr)}
}

func (d DefaultDataFrame) Filter(expr LogicalExpr) DataFrame {
	return DefaultDataFrame{NewSelection(d.plan, expr)}
}

func (d DefaultDataFrame) Aggregate(groupBy []LogicalExpr, aggExpr []AggregateExpr) DataFrame {
	return DefaultDataFrame{NewAggregate(d.plan, groupBy, aggExpr)}
}

func (d DefaultDataFrame) Schema() datatypes.Schema {
	return d.plan.Schema()
}

func (d DefaultDataFrame) LogicalPlan() LogicalPlan {
	return d.plan
}

func NewDefaultDataFrame(plan LogicalPlan) DefaultDataFrame {
	return DefaultDataFrame{plan}
}
