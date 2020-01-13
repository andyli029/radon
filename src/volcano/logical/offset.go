package logical

import (
	"context"
	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type Offset struct {
	data       Node
	offsetExpr Expression
}

func NewOffset(data Node, expr Expression) Node {
	return &Offset{data: data, offsetExpr: expr}
}

func (node *Offset) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	dataNode, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
	}

	offsetExpr, offsetVariables, err := node.offsetExpr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for offset expression")
	}
	variables, err = variables.MergeWith(offsetVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get offset node variables")
	}

	return physical.NewOffset(dataNode, offsetExpr), variables, nil
}
