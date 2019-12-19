package execution

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"volcano/common"
)

type Function struct {
	Name          string
	ArgumentNames [][]string
	Description   common.Documentation
	Validator     Validator
	Logic         func(...common.Value) (common.Value, error)
}

func (f *Function) Document() common.Documentation {
	callingWays := make([]common.Documentation, len(f.ArgumentNames))
	for i, arguments := range f.ArgumentNames {
		callingWays[i] = common.Text(fmt.Sprintf("%s(%s)", f.Name, strings.Join(arguments, ", ")))
	}
	return common.Section(
		f.Name,
		common.Body(
			common.Section("Calling", common.List(callingWays...)),
			common.Section("Arguments", f.Validator.Document()),
			common.Section("Description", f.Description),
		),
	)
}

type Validator interface {
	common.Documented
	Validate(args ...common.Value) error
}

type FunctionExpression struct {
	function  *Function
	arguments []Expression
}

func NewFunctionExpression(fun *Function, args []Expression) *FunctionExpression {
	return &FunctionExpression{
		function:  fun,
		arguments: args,
	}
}

func (fe *FunctionExpression) ExpressionValue(variables common.Variables) (common.Value, error) {
	values := make([]common.Value, 0)
	for i := range fe.arguments {
		value, err := fe.arguments[i].ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't get value of function %v argument with index %v", fe.function.Name, i)
		}

		values = append(values, value)
	}

	err := fe.function.Validator.Validate(values...)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid arguments to function %v", fe.function.Name)
	}

	finalValue, err := fe.function.Logic(values...)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get function %v value", fe.function.Name)
	}

	return finalValue, nil
}
