package logical

import (
	"context"
	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type Limit struct {
	data      Node
	limitExpr Expression
}

func NewLimit(data Node, expr Expression) Node {
	return &Limit{data: data, limitExpr: expr}
}

func (node *Limit) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	dataNode, variables, err := node.data.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for data node")
	}

	limitExpr, limitVariables, err := node.limitExpr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for limit expression")
	}
	variables, err = variables.MergeWith(limitVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get limit node variables")
	}

	return physical.NewLimit(dataNode, limitExpr), variables, nil
}
