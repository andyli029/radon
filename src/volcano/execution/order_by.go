package execution

import (
	"reflect"
	"sort"

	"volcano/common"
	"github.com/pkg/errors"
)

type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

type OrderBy struct {
	expressions []Expression
	directions  []OrderDirection
	source      Node
}

func NewOrderBy(exprs []Expression, directions []OrderDirection, source Node) *OrderBy {
	return &OrderBy{
		expressions: exprs,
		directions:  directions,
		source:      source,
	}
}

func isSorteable(x common.Value) bool {
	switch x.(type) {
	case common.Bool:
		return true
	case common.Int:
		return true
	case common.Float:
		return true
	case common.String:
		return true
	case common.Time:
		return true
	case common.Null, common.Phantom, common.Duration, common.Tuple, common.Object:
		return false
	}

	panic("unreachable")
}

func compare(x, y common.Value) (int, error) {
	switch x := x.(type) {
	case common.Int:
		y, ok := y.(common.Int)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case common.Float:
		y, ok := y.(common.Float)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case common.String:
		y, ok := y.(common.String)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case common.Time:
		y, ok := y.(common.Time)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if x.AsTime().Before(y.AsTime()) {
			return -1, nil
		}

		return 1, nil
	case common.Bool:
		y, ok := y.(common.Bool)
		if !ok {
			return 0, errors.Errorf("type mismatch between values")
		}

		if x == y {
			return 0, nil
		} else if !x && y {
			return -1, nil
		}

		return 1, nil

	case common.Null, common.Phantom, common.Duration, common.Tuple, common.Object:
		return 0, errors.Errorf("unsupported type in sorting")
	}

	panic("unreachable")
}

func (ob *OrderBy) Get(variables common.Variables) (RecordStream, error) {
	sourceStream, err := ob.source.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get underlying stream in order by")
	}

	orderedStream, err := createOrderedStream(ob.expressions, ob.directions, variables, sourceStream)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create ordered stream from source stream")
	}

	return orderedStream, nil
}

func createOrderedStream(expressions []Expression, directions []OrderDirection, variables common.Variables, sourceStream RecordStream) (stream RecordStream, outErr error) {
	records := make([]*Record, 0)

	for {
		rec, err := sourceStream.Next()
		if err == ErrEndOfStream {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "couldn't get all records")
		}

		records = append(records, rec)
	}

	defer func() {
		if err := recover(); err != nil {
			stream = nil
			outErr = errors.Wrap(err.(error), "couldn't sort records")
		}
	}()
	sort.Slice(records, func(i, j int) bool {
		iRec := records[i]
		jRec := records[j]

		for num, expr := range expressions {
			// TODO: Aggressive caching of these expressions...
			iVars, err := variables.MergeWith(iRec.AsVariables())
			if err != nil {
				panic(errors.Wrap(err, "couldn't merge variables"))
			}
			jVars, err := variables.MergeWith(jRec.AsVariables())
			if err != nil {
				panic(errors.Wrap(err, "couldn't merge variables"))
			}

			x, err := expr.ExpressionValue(iVars)
			if err != nil {
				panic(errors.Wrapf(err, "couldn't get order by expression with index %v value", num))
			}
			y, err := expr.ExpressionValue(jVars)
			if err != nil {
				panic(errors.Wrapf(err, "couldn't get order by expression with index %v value", num))
			}

			if !isSorteable(x) {
				panic(errors.Errorf("value %v of type %v is not comparable", x, reflect.TypeOf(x).String()))
			}
			if !isSorteable(y) {
				panic(errors.Errorf("value %v of type %v is not comparable", y, reflect.TypeOf(y).String()))
			}

			cmp, err := compare(x, y)
			if err != nil {
				panic(errors.Errorf("failed to compare values %v and %v", x, y))
			}

			answer := false

			if cmp == 0 {
				continue
			} else if cmp > 0 {
				answer = true
			}

			if directions[num] == Ascending {
				answer = !answer
			}

			return answer
		}

		return false
	})

	return NewInMemoryStream(records), nil
}
