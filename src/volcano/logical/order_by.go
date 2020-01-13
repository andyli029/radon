package logical

import (
	"context"

	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type OrderDirection string

type OrderBy struct {
	expressions []Expression
	directions  []OrderDirection
	source      Node
}

func NewOrderBy(expressions []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		expressions: expressions,
		directions:  directions,
		source:      source,
	}
}

func (node *OrderBy) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	sourceNode, variables, err := node.source.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan of source node in order by")
	}

	expressions := make([]physical.Expression, len(node.expressions))
	for i := range node.expressions {
		expr, exprVariables, err := node.expressions[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't get physical plan for order by expression with index %d", i)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "couldn't merge variables with those of order by expression with index %d", i)
		}

		expressions[i] = expr
	}

	directions := make([]physical.OrderDirection, len(node.expressions))
	for i, direction := range node.directions {
		switch direction {
		case "asc":
			directions[i] = physical.Ascending
		case "desc":
			directions[i] = physical.Descending
		default:
			return nil, nil, errors.Errorf("invalid order by direction: %v", direction)
		}
	}

	return physical.NewOrderBy(expressions, directions, sourceNode), variables, nil
}
