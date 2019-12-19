/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"context"
	"fmt"
	"os"

	"volcano/common"
	"volcano/execution"
	"volcano/logical"
	"volcano/output"
	"volcano/output/table"
	"volcano/parser"
	"volcano/physical"
	"volcano/storage/highavailable_mysql"
	"volcano/storage/mysql"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	//"github.com/xelabs/go-mysqlstack/xlog"
)

// ExecuteVolcano used to Execute Volcano.
func ExecuteVolcano(node sqlparser.Statement, database string, spanner *Spanner) (*sqltypes.Result, error) {
	switch node.(type) {
	case *sqlparser.Select:
		nod := node.(*sqlparser.Select)
		//selectNode := planner.NewSelectPlan(log, database, query, nod, router)
		//plans.Add(selectNode)
		//qr := &sqltypes.Result{}
		qr, err := VolcanoBuildRadon(nod, database, spanner)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't create physical plan")
		}
		return qr, nil
	default:
		return nil, errors.Errorf("optimizer.unsupported.query.type[%+v]", node)
	}

	return nil, nil
}

type App struct {
	cfg                  *common.Config
	dataSourceRepository *physical.DataSourceRepository
	out                  output.Output
}

func NewApp(cfg *common.Config, dataSourceRepository *physical.DataSourceRepository, out output.Output) *App {
	return &App{
		cfg:                  cfg,
		dataSourceRepository: dataSourceRepository,
		out:                  out,
	}
}

/*
func (app *App) RunPlan(ctx context.Context, plan logical.Node, spanner *Spanner, database string) (*sqltypes.Result, error)  {
	phys, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create physical plan")
	}

	//phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(app.cfg, nil))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, err := exec.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	qr := &sqltypes.Result{}
	var rec *execution.Record
	fieldMark := false
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		//qr.AppendResult(innerqr)
		//err := app.out.WriteRecord(rec)
		//if err != nil {
		//	return nil, errors.Wrap(err, "couldn't write record")
		//}

		//qr.Rows = append(qr.Rows, (querypb.Value{})rec.data)
		result := make([]sqltypes.Value, len(rec.Fields()))
		for i, line := range rec.Data() {
			result[i] = sqltypes.MakeTrusted(typeOtoR(line), []byte(fmt.Sprintf("%s", line.String())))
		}

		qr.Rows = append(qr.Rows, result)
		qr.RowsAffected++

		for i, f := range rec.Fields(){
			if fieldMark != false {
				break
			}
			qr.Fields = append(qr.Fields, &querypb.Field{Name: string(f.Name), Type: typeOtoR(rec.Data()[i])})
		}
		fieldMark = true
	}

	//if err != execution.ErrEndOfStream {
	//	return nil, errors.Wrap(err, "couldn't get next record")
	//}

	//err = app.out.Close()
	//if err != nil {
	//	return nil, errors.Wrap(err, "couldn't close output writer")
	//}

	return qr, nil
}
*/

func (app *App) RunPlanRadon(ctx context.Context, plan logical.Node, spanner *Spanner) (*sqltypes.Result, error)  {
	phys, variables, err := plan.Physical(ctx, logical.NewPhysicalPlanCreator(app.dataSourceRepository))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create physical plan")
	}

	//phys = optimizer.Optimize(ctx, optimizer.DefaultScenarios, phys)

	exec, err := phys.Materialize(ctx, physical.NewMaterializationContext(app.cfg, spanner.Router(), spanner.Scatter()))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't materialize the physical plan into an execution plan")
	}

	stream, err := exec.Get(variables)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get record stream from execution plan")
	}

	qr := &sqltypes.Result{}
	var rec *execution.Record
	fieldMark := false
	for rec, err = stream.Next(); err == nil; rec, err = stream.Next() {
		//qr.AppendResult(innerqr)
		//err := app.out.WriteRecord(rec)
		//if err != nil {
		//	return nil, errors.Wrap(err, "couldn't write record")
		//}

		//qr.Rows = append(qr.Rows, (querypb.Value{})rec.data)
		if rec == nil {
			continue
		}

		result := make([]sqltypes.Value, len(rec.Fields()))
		for i, line := range rec.Data() {
			result[i] = sqltypes.MakeTrusted(typeOtoR(line), []byte(fmt.Sprintf("%s", line.String())))
		}

		qr.Rows = append(qr.Rows, result)
		qr.RowsAffected++

		for i, f := range rec.Fields(){
			if fieldMark != false {
				break
			}
			qr.Fields = append(qr.Fields, &querypb.Field{Name: string(f.Name), Type: typeOtoR(rec.Data()[i])})
		}
		fieldMark = true
	}

	//if err != execution.ErrEndOfStream {
	//	return nil, errors.Wrap(err, "couldn't get next record")
	//}

	//err = app.out.Close()
	//if err != nil {
	//	return nil, errors.Wrap(err, "couldn't close output writer")
	//}

	return qr, nil
}

func typeOtoR(value common.Value) querypb.Type {
	switch value.(type) {
	case common.Int:
		return querypb.Type_INT32
	case common.Float:
		return querypb.Type_FLOAT64
	case common.String:
		return querypb.Type_VARCHAR
	case common.Time:
		return querypb.Type_TIME
	case common.Duration:
		return querypb.Type_FLOAT64
	case common.Tuple:
		return querypb.Type_TUPLE
	default:
		panic("unreachable")
	}
}

var outputFormat string

/*
func VolcanoBuild(node *sqlparser.Select, spanner *Spanner) (*sqltypes.Result, error) {
	var err error
	// Check subquery.
	//if hasSubquery(node) {
	//	return errors.New("unsupported: subqueries.in.select")
	//}
	ctx := context.Background()

	cfg, err := common.ReadConfig("/Users/andy/octo.yaml")
	if err != nil {
		return nil, err
	}
	dataSourceRespository, err := physical.CreateDataSourceRepositoryFromConfig(
		map[string]physical.Factory{
			"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
			"highavailable_mysql":    highavailable_mysql.NewDataSourceBuilderFactoryFromConfig,
		},
		cfg,
	)

	var out output.Output
	switch outputFormat {
	case "table":
		out = table.NewOutput(os.Stdout, false)
	default:
	}

	app := NewApp(cfg, dataSourceRespository, out)

	plan, err := parser.ParseSelect(node)
	if err != nil {
		//log.Fatal("couldn't parse query: ", err)
	}

	// Run query
	qr, err := app.RunPlan(ctx, plan, spanner)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create physical plan")
	}

	return qr, nil
}
*/

func VolcanoBuildRadon(node *sqlparser.Select, database string, spanner *Spanner) (*sqltypes.Result, error) {
	var err error
	// Check subquery.
	//if hasSubquery(node) {
	//	return errors.New("unsupported: subqueries.in.select")
	//}
	ctx := context.Background()

	cfg, err := common.ReadConfig("/Users/andy/octo_bak.yaml")
	if err != nil {
		return nil, err
	}
	dataSourceRespository, err := physical.CreateDataSourceRepositoryFromConfig(
		map[string]physical.Factory{
			"mysql":    mysql.NewDataSourceBuilderFactoryFromConfig,
			"highavailable_mysql":    highavailable_mysql.NewDataSourceBuilderFactoryFromConfig,
		},
		cfg,
	)

	var out output.Output
	switch outputFormat {
	case "table":
		out = table.NewOutput(os.Stdout, false)
	default:
	}

	app := NewApp(cfg, dataSourceRespository, out)

	plan, err := parser.ParseSelect(node, database)
	if err != nil {
		//log.Fatal("couldn't parse query: ", err)
	}

	// Run query
	qr, err := app.RunPlanRadon(ctx, plan, spanner)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create physical plan")
	}

	return qr, nil
}
