package logical

import (
	"context"
	"fmt"

	"volcano/common"
	"volcano/physical"
	"github.com/pkg/errors"
)

type PhysicalPlanCreator struct {
	variableCounter int
	dataSourceRepo  *physical.DataSourceRepository
/*	database string*/
}

func NewPhysicalPlanCreator(repo *physical.DataSourceRepository/*, database string*/) *PhysicalPlanCreator {
	return &PhysicalPlanCreator{
		variableCounter: 0,
		dataSourceRepo:  repo,
	}
}

func (creator *PhysicalPlanCreator) GetVariableName() (out common.VariableName) {
	out = common.NewVariableName(fmt.Sprintf("const_%d", creator.variableCounter))
	creator.variableCounter++
	return
}

type Node interface {
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error)
}

type DataSource struct {
	database string
	name  string
	alias string
}

func NewDataSource(name string, alias string) *DataSource {
	return &DataSource{name: name, alias: alias}
}

func NewDataSourceWithDatabase(name string, alias string, database string) *DataSource {
	return &DataSource{database: database, name: name, alias: alias}
}

func (ds *DataSource) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Node, common.Variables, error) {
	outDs, err := physicalCreator.dataSourceRepo.Get(ds.name, ds.alias, ds.database)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get data source")
	}
	return outDs, common.NoVariables(), nil
}

type Expression interface {
	Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error)
}

type NamedExpression interface {
	Expression
	Name() common.VariableName
	PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, common.Variables, error)
}

type Variable struct {
	name common.VariableName
}

func NewVariable(name common.VariableName) *Variable {
	return &Variable{name: name}
}

func (v *Variable) Name() common.VariableName {
	return v.name
}

func (v *Variable) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	return v.PhysicalNamed(ctx, physicalCreator)
}

func (v *Variable) PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, common.Variables, error) {
	return physical.NewVariable(v.name), common.NoVariables(), nil
}

type Constant struct {
	value interface{}
}

func NewConstant(value interface{}) *Constant {
	return &Constant{value: value}
}

func (v *Constant) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	name := physicalCreator.GetVariableName()
	return physical.NewVariable(name), common.NewVariables(map[common.VariableName]common.Value{
		name: common.NormalizeType(v.value),
	}), nil
}

type Tuple struct {
	expressions []Expression
}

func NewTuple(expressions []Expression) *Tuple {
	return &Tuple{expressions: expressions}
}

func (tup *Tuple) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	physicalExprs := make([]physical.Expression, len(tup.expressions))
	variables := common.NoVariables()
	for i := range tup.expressions {
		physicalExpr, exprVariables, err := tup.expressions[i].Physical(ctx, physicalCreator)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't get physical plan for tuple subexpression with index %d", i,
			)
		}
		variables, err = variables.MergeWith(exprVariables)
		if err != nil {
			return nil, nil, errors.Wrapf(
				err,
				"couldn't merge variables with those of tuple subexpression with index %d", i,
			)
		}

		physicalExprs[i] = physicalExpr
	}
	return physical.NewTuple(physicalExprs), variables, nil
}

type NodeExpression struct {
	node Node
}

func NewNodeExpression(node Node) *NodeExpression {
	return &NodeExpression{node: node}
}

func (ne *NodeExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	physicalNode, variables, err := ne.node.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for node expression")
	}
	return physical.NewNodeExpression(physicalNode), variables, nil
}

type LogicExpression struct {
	formula Formula
}

func NewLogicExpression(formula Formula) *LogicExpression {
	return &LogicExpression{formula: formula}
}

func (le *LogicExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	physicalNode, variables, err := le.formula.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for logic expression")
	}
	return physical.NewLogicExpression(physicalNode), variables, nil
}

type AliasedExpression struct {
	name common.VariableName
	expr Expression
}

func NewAliasedExpression(name common.VariableName, expr Expression) NamedExpression {
	return &AliasedExpression{name: name, expr: expr}
}

func (alExpr *AliasedExpression) Name() common.VariableName {
	return alExpr.name
}

func (alExpr *AliasedExpression) Physical(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.Expression, common.Variables, error) {
	return alExpr.PhysicalNamed(ctx, physicalCreator)
}

func (alExpr *AliasedExpression) PhysicalNamed(ctx context.Context, physicalCreator *PhysicalPlanCreator) (physical.NamedExpression, common.Variables, error) {
	physicalNode, variables, err := alExpr.expr.Physical(ctx, physicalCreator)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't get physical plan for aliased expression")
	}
	return physical.NewAliasedExpression(alExpr.name, physicalNode), variables, nil
}
