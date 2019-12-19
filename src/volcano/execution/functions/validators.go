package functions

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"volcano/common"
	. "volcano/execution"
)

type SingleArgumentValidator interface {
	common.Documented
	Validate(arg common.Value) error
}

type all struct {
	validators []Validator
}

func All(validators ...Validator) *all {
	return &all{validators: validators}
}

func (v *all) Validate(args ...common.Value) error {
	for _, validator := range v.validators {
		err := validator.Validate(args...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *all) Document() common.Documentation {
	childDocs := make([]common.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}
	return common.List(childDocs...)
}

type singleAll struct {
	validators []SingleArgumentValidator
}

func SingleAll(validators ...SingleArgumentValidator) *singleAll {
	return &singleAll{validators: validators}
}

func (v *singleAll) Validate(args common.Value) error {
	for _, validator := range v.validators {
		err := validator.Validate(args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *singleAll) Document() common.Documentation {
	childDocs := make([]common.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}
	return common.List(childDocs...)
}

type oneOf struct {
	validators []Validator
}

func OneOf(validators ...Validator) *oneOf {
	return &oneOf{validators: validators}
}

func (v *oneOf) Validate(args ...common.Value) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(args...)
		if errs[i] == nil {
			return nil
		}
	}

	return fmt.Errorf("none of the conditions have been met: %+v", errs)
}

func (v *oneOf) Document() common.Documentation {
	childDocs := make([]common.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}

	return common.Paragraph(common.Text("must satisfy one of"), common.List(childDocs...))
}

type singleOneOf struct {
	validators []SingleArgumentValidator
}

func SingleOneOf(validators ...SingleArgumentValidator) *singleOneOf {
	return &singleOneOf{validators: validators}
}

func (v *singleOneOf) Validate(arg common.Value) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(arg)
		if errs[i] == nil {
			return nil
		}
	}

	return fmt.Errorf("none of the conditions have been met: %+v", errs)
}

func (v *singleOneOf) Document() common.Documentation {
	childDocs := make([]common.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}

	return common.Paragraph(common.Text("must satisfy one of the following"), common.List(childDocs...))
}

type ifArgPresent struct {
	i         int
	validator Validator
}

func IfArgPresent(i int, validator Validator) *ifArgPresent {
	return &ifArgPresent{i: i, validator: validator}
}

func (v *ifArgPresent) Validate(args ...common.Value) error {
	if len(args) < v.i+1 {
		return nil
	}
	return v.validator.Validate(args...)
}

func (v *ifArgPresent) Document() common.Documentation {
	return common.Paragraph(
		common.Text(fmt.Sprintf("if the %s argument is provided, then", common.Ordinal(v.i+1))),
		v.validator.Document(),
	)
}

type atLeastNArgs struct {
	n int
}

func AtLeastNArgs(n int) *atLeastNArgs {
	return &atLeastNArgs{n: n}
}

func (v *atLeastNArgs) Validate(args ...common.Value) error {
	if len(args) < v.n {
		return fmt.Errorf("expected at least %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

func (v *atLeastNArgs) Document() common.Documentation {
	return common.Text(fmt.Sprintf("at least %s may be provided", argumentCount(v.n)))
}

type atMostNArgs struct {
	n int
}

func AtMostNArgs(n int) *atMostNArgs {
	return &atMostNArgs{n: n}
}

func (v *atMostNArgs) Validate(args ...common.Value) error {
	if len(args) > v.n {
		return fmt.Errorf("expected at most %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

func (v *atMostNArgs) Document() common.Documentation {
	return common.Text(fmt.Sprintf("at most %s may be provided", argumentCount(v.n)))
}

type exactlyNArgs struct {
	n int
}

func ExactlyNArgs(n int) *exactlyNArgs {
	return &exactlyNArgs{n: n}
}

func (v *exactlyNArgs) Validate(args ...common.Value) error {
	if len(args) != v.n {
		return fmt.Errorf("expected exactly %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

func (v *exactlyNArgs) Document() common.Documentation {
	return common.Text(fmt.Sprintf("exactly %s must be provided", argumentCount(v.n)))
}

type typeOf struct {
	wantedType common.Value
}

func TypeOf(wantedType common.Value) *typeOf {
	return &typeOf{wantedType: wantedType}
}

func (v *typeOf) Validate(arg common.Value) error {
	switch v.wantedType.(type) {
	case common.Null:
		if _, ok := arg.(common.Null); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroNull()).String(), arg)
		}
		return nil

	case common.Phantom:
		if _, ok := arg.(common.Phantom); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroPhantom()).String(), arg)
		}
		return nil

	case common.Int:
		if _, ok := arg.(common.Int); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroInt()).String(), arg)
		}
		return nil

	case common.Float:
		if _, ok := arg.(common.Float); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroFloat()).String(), arg)
		}
		return nil

	case common.Bool:
		if _, ok := arg.(common.Bool); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroBool()).String(), arg)
		}
		return nil

	case common.String:
		if _, ok := arg.(common.String); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroString()).String(), arg)
		}
		return nil

	case common.Time:
		if _, ok := arg.(common.Time); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroTime()).String(), arg)
		}
		return nil

	case common.Duration:
		if _, ok := arg.(common.Duration); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroDuration()).String(), arg)
		}
		return nil

	case common.Tuple:
		if _, ok := arg.(common.Tuple); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroTuple()).String(), arg)
		}
		return nil

	case common.Object:
		if _, ok := arg.(common.Object); !ok {
			return fmt.Errorf("expected type %v but got %v", reflect.TypeOf(common.ZeroObject()).String(), arg)
		}
		return nil

	}

	log.Fatalf("unhandled type: %v", reflect.TypeOf(v.wantedType).String())
	panic("unreachable")
}

func (v *typeOf) Document() common.Documentation {
	return common.Paragraph(common.Text("must be of type"), v.wantedType.Document())
}

type valueOf struct {
	values []common.Value
}

func ValueOf(values ...common.Value) *valueOf {
	return &valueOf{values: values}
}

func (v *valueOf) Validate(arg common.Value) error {
	for i := range v.values {
		if common.AreEqual(v.values[i], arg) {
			return nil
		}
	}

	values := make([]string, len(v.values))
	for i := range v.values {
		values[i] = v.values[i].String()
	}

	return fmt.Errorf(
		"argument must be one of: [%s], got %v",
		strings.Join(values, ", "),
		arg,
	)
}

func (v *valueOf) Document() common.Documentation {
	values := make([]string, len(v.values))
	for i := range v.values {
		values[i] = v.values[i].String()
	}
	return common.Paragraph(
		common.Text(
			fmt.Sprintf(
				"must be one of: [%s]",
				strings.Join(values, ", "),
			),
		),
	)
}

type arg struct {
	i         int
	validator SingleArgumentValidator
}

func Arg(i int, validator SingleArgumentValidator) *arg {
	return &arg{i: i, validator: validator}
}

func (v *arg) Validate(args ...common.Value) error {
	if err := v.validator.Validate(args[v.i]); err != nil {
		return fmt.Errorf("bad argument at index %v: %v", v.i, err)
	}
	return nil
}

func (v *arg) Document() common.Documentation {
	return common.Paragraph(
		common.Text(fmt.Sprintf("the %s argument", common.Ordinal(v.i+1))),
		v.validator.Document(),
	)
}

type allArgs struct {
	validator SingleArgumentValidator
}

func AllArgs(validator SingleArgumentValidator) *allArgs {
	return &allArgs{validator: validator}
}

func (v *allArgs) Validate(args ...common.Value) error {
	for i := range args {
		if err := v.validator.Validate(args[i]); err != nil {
			return fmt.Errorf("bad argument at index %v: %v", i, err)
		}
	}
	return nil
}

func (v *allArgs) Document() common.Documentation {
	return common.Paragraph(
		common.Text("all arguments"),
		v.validator.Document(),
	)
}

func argumentCount(n int) string {
	switch n {
	case 1:
		return "1 argument"
	default:
		return fmt.Sprintf("%d arguments", n)
	}
}
