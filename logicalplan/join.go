package logicalplan

import (
	"query-engine/datatypes"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type Join struct {
	left     LogicalPlan
	right    LogicalPlan
	joinType JoinType

	// on list item is a pair that contains on condition left and right fields
	on [][]string
}

func (j Join) Schema() datatypes.Schema {
	panic("implement me")
}

func (j Join) Children() []LogicalPlan {
	panic("implement me")
}

func (j Join) String() string {
	panic("implement me")
}
