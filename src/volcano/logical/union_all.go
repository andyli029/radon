package logical

import (
	"context"

	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type UnionAll struct {
	first, second Node
}

func NewUnionAll(first, second Node) *UnionAll {
	return &UnionAll{first: first, second: second}
}

func (node *UnionAll) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	variables := common.NoVariables()
	firstNode, firstVariables, err := node.first.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for first node")
	}
	variables, err = variables.MergeWith(firstVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get first node variables")
	}

	secondNode, secondVariables, err := node.second.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for second node")
	}
	variables, err = variables.MergeWith(secondVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get second node variables")
	}

	return physical.NewUnionAll(firstNode, secondNode), variables, nil
}
