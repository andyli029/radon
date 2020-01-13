package physical

import (
	"context"

	"volcano/common"
	"volcano/execution"
	"volcano/physical/metadata"
	"github.com/pkg/errors"
)

type LeftJoin struct {
	Source Node
	Joined Node
}

func NewLeftJoin(source Node, joined Node) *LeftJoin {
	return &LeftJoin{Source: source, Joined: joined}
}

func (node *LeftJoin) Transform(ctx context.Context, transformers *Transformers) Node {
	var transformed Node = &LeftJoin{
		Source: node.Source.Transform(ctx, transformers),
		Joined: node.Joined.Transform(ctx, transformers),
	}
	if transformers.NodeT != nil {
		transformed = transformers.NodeT(transformed)
	}
	return transformed
}

func (node *LeftJoin) Materialize(ctx context.Context, matCtx *MaterializationContext) (execution.Node, error) {
	prefetchCount, err := common.GetInt(matCtx.Config.Execution, "lookupJoinPrefetchCount", common.WithDefault(32))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get lookupJoinPrefetchCount configuration")
	}

	materializedSource, err := node.Source.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize source node")
	}

	materializedJoined, err := node.Joined.Materialize(ctx, matCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize joined node")
	}

	return execution.NewLeftJoin(prefetchCount, materializedSource, materializedJoined), nil
}

func (node *LeftJoin) Metadata() *metadata.NodeMetadata {
	return metadata.NewNodeMetadata(metadata.CombineCardinalities(node.Source.Metadata().Cardinality(), node.Joined.Metadata().Cardinality()))
}
