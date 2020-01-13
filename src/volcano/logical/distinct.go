package logical

import (
	"context"

	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type Distinct struct {
	child Node
}

func NewDistinct(child Node) *Distinct {
	return &Distinct{child: child}
}

func (node *Distinct) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	childNode, variables, err := node.child.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get child's physical plan in distinct")
	}

	return physical.NewDistinct(childNode), variables, nil
}
