package logical

import (
	"context"
	"github.com/pkg/errors"
	"volcano/common"
	"volcano/physical"
)

type LeftJoin struct {
	source Node
	joined Node
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{source: source, joined: joined}
}

func (node *LeftJoin) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	source, sourceVariables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map source node")
	}

	joined, joinedVariables, err := node.joined.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for map joined node")
	}

	variables, err := sourceVariables.MergeWith(joinedVariables)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't merge variables for source and joined nodes")
	}

	return physical.NewLeftJoin(source, joined), variables, nil
}
