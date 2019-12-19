package logical

import (
	"context"

	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type Filter struct {
	formula Formula
	source  Node
}

func NewFilter(formula Formula, child Node) *Filter {
	return &Filter{formula: formula, source: child}
}

func (node *Filter) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	formula, formulaVariables, err := node.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for formula")
	}
	child, childVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for filter source node")
	}

	variables, err := childVariables.MergeWith(formulaVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for filter source")
	}

	return physical.NewFilter(formula, child), variables, nil
}
