package logical

import (
	"context"
	"volcano/common"
	"volcano/physical"
)

type UnionDistinct struct {
	first, second Node
}

func NewUnionDistinct(first, second Node) *UnionDistinct {
	return &UnionDistinct{first: first, second: second}
}

func (node *UnionDistinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	return NewDistinct(NewUnionAll(node.first, node.second)).Physical(ctx, physicalCreator)
}
