package execution

import (
	"fmt"

	"volcano/common"
	"github.com/pkg/errors"
)

type AggregatePrototype func() Aggregate

type Aggregate interface {
	common.Documented
	AddRecord(key common.Tuple, value common.Value) error
	GetAggregated(key common.Tuple) (common.Value, error)
	String() string
}

type GroupBy struct {
	source Node
	key    []Expression

	fields              []common.VariableName
	aggregatePrototypes []AggregatePrototype

	as []common.VariableName
}

func NewGroupBy(source Node, key []Expression, fields []common.VariableName, aggregatePrototypes []AggregatePrototype, as []common.VariableName) *GroupBy {
	return &GroupBy{source: source, key: key, fields: fields, aggregatePrototypes: aggregatePrototypes, as: as}
}

func (node *GroupBy) Get(variables common.Variables) (RecordStream, error) {
	source, err := node.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get stream for source in group by")
	}

	aggregates := make([]Aggregate, len(node.aggregatePrototypes))
	for i := range node.aggregatePrototypes {
		aggregates[i] = node.aggregatePrototypes[i]()
	}

	return &GroupByStream{
		source:    source,
		variables: variables,

		key:    node.key,
		groups: NewHashMap(),

		fields:     node.fields,
		aggregates: aggregates,

		as: node.as,
	}, nil
}

type GroupByStream struct {
	source    RecordStream
	variables common.Variables

	key    []Expression
	groups *HashMap

	fields     []common.VariableName
	aggregates []Aggregate

	as []common.VariableName

	fieldNames []common.VariableName
	iterator   *Iterator
}

func (stream *GroupByStream) Next() (*Record, error) {
	if stream.iterator == nil {
		for {
			record, err := stream.source.Next()
			if err != nil {
				if err == ErrEndOfStream {
					stream.fieldNames = make([]common.VariableName, len(stream.fields))
					for i := range stream.fields {
						if len(stream.as[i]) > 0 {
							stream.fieldNames[i] = stream.as[i]
						} else {
							stream.fieldNames[i] = common.NewVariableName(
								fmt.Sprintf(
									"%s_%s",
									stream.fields[i].String(),
									stream.aggregates[i].String(),
								),
							)
						}
					}
					stream.iterator = stream.groups.GetIterator()
					break
				}
				return nil, errors.Wrap(err, "couldn't get next source record")
			}

			variables, err := stream.variables.MergeWith(record.AsVariables())
			if err != nil {
				return nil, errors.Wrap(err, "couldn't merge stream variables with record")
			}

			key := make(common.Tuple, len(stream.key))
			for i := range stream.key {
				key[i], err = stream.key[i].ExpressionValue(variables)
				if err != nil {
					return nil, errors.Wrapf(err, "couldn't evaluate group key expression with index %v", i)
				}
			}

			if len(key) == 0 {
				key = append(key, common.Phantom{})
			}

			err = stream.groups.Set(key, common.Phantom{})
			if err != nil {
				return nil, errors.Wrap(err, "couldn't put group key into hashmap")
			}

			for i := range stream.aggregates {
				var value common.Value
				if stream.fields[i] == "*star*" {
					mapping := make(common.Object, len(record.Fields()))
					for _, field := range record.Fields() {
						mapping[field.Name.String()] = record.Value(field.Name)
					}
					value = mapping

				} else {
					value = record.Value(stream.fields[i])
				}
				err := stream.aggregates[i].AddRecord(key, value)
				if err != nil {
					return nil, errors.Wrapf(
						err,
						"couldn't add record value to aggregate %s with index %v",
						stream.aggregates[i].String(),
						i,
					)
				}
			}
		}
	}

	key, _, ok := stream.iterator.Next()
	if !ok {
		return nil, ErrEndOfStream
	}
	typedKey := key.(common.Tuple)

	values := make([]common.Value, len(stream.aggregates))
	for i := range stream.aggregates {
		var err error
		values[i], err = stream.aggregates[i].GetAggregated(typedKey)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get aggregate value")
		}
	}

	return NewRecordFromSlice(stream.fieldNames, values), nil
}

func (stream *GroupByStream) Close() error {
	return stream.source.Close()
}
