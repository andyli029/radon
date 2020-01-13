package execution

import (
	"volcano/common"
	"github.com/pkg/errors"
)

type Formula interface {
	Evaluate(variables common.Variables) (bool, error)
}

type Constant struct {
	Value bool
}

func NewConstant(value bool) *Constant {
	return &Constant{Value: value}
}

func (f Constant) Evaluate(variables common.Variables) (bool, error) {
	return f.Value, nil
}

type And struct {
	Left, Right Formula
}

func NewAnd(left Formula, right Formula) *And {
	return &And{Left: left, Right: right}
}

func (f *And) Evaluate(variables common.Variables) (bool, error) {
	left, err := f.Left.Evaluate(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't evaluate left operand in and")
	}
	right, err := f.Right.Evaluate(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't evaluate right operand in and")
	}

	return left && right, nil
}

type Or struct {
	Left, Right Formula
}

func NewOr(left Formula, right Formula) *Or {
	return &Or{Left: left, Right: right}
}

func (f *Or) Evaluate(variables common.Variables) (bool, error) {
	left, err := f.Left.Evaluate(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't evaluate left operand in or")
	}

	right, err := f.Right.Evaluate(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't evaluate right operand in or")
	}

	return left || right, nil
}

type Not struct {
	Child Formula
}

func NewNot(child Formula) *Not {
	return &Not{Child: child}
}

func (f *Not) Evaluate(variables common.Variables) (bool, error) {
	child, err := f.Child.Evaluate(variables)
	if err != nil {
		return false, errors.Wrap(err, "couldn't evaluate child formula in not")
	}

	return !child, nil
}

type Predicate struct {
	Left     Expression
	Relation Relation
	Right    Expression
}

func NewPredicate(left Expression, relation Relation, right Expression) *Predicate {
	return &Predicate{Left: left, Relation: relation, Right: right}
}

func (f *Predicate) Evaluate(variables common.Variables) (bool, error) {
	return f.Relation.Apply(variables, f.Left, f.Right)
}
