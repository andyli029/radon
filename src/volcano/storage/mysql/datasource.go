package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"volcano/common"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"volcano/execution"
	"volcano/physical"
	"volcano/physical/metadata"
)

var availableFilters = map[physical.FieldType]map[physical.Relation]struct{}{
	physical.Primary: {
		physical.Equal:        {},
		physical.NotEqual:     {},
		physical.MoreThan:     {},
		physical.LessThan:     {},
		physical.GreaterEqual: {},
		physical.LessEqual:    {},
		physical.Like:         {},
	},
	physical.Secondary: {
		physical.Equal:        {},
		physical.NotEqual:     {},
		physical.MoreThan:     {},
		physical.LessThan:     {},
		physical.Like:         {},
		physical.GreaterEqual: {},
		physical.LessEqual:    {},
	},
}

type DataSource struct {
	db      *sql.DB
	stmt    *sql.Stmt
	aliases []execution.Expression
	alias   string
}

func NewDataSourceBuilderFactory(primaryKeys []common.VariableName) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, dsb *physical.DataSourceBuilder) (execution.Node, error) {
			// Get execution configuration
			host, port, err := common.GetIPAddress(dbConfig, "address", common.WithDefault([]interface{}{"localhost", 3306}))
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get address")
			}
			user, err := common.GetString(dbConfig, "user")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get user")
			}
			databaseName, err := common.GetString(dbConfig, "databaseName")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get databaseName")
			}
			tableName, err := common.GetString(dbConfig, "tableName")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get tableName")
			}
			password, err := common.GetString(dbConfig, "password")
			if err != nil {
				return nil, errors.Wrap(err, "couldn't get password")
			}

			// Build dsn
			mysqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, databaseName)

			db, err := sql.Open("mysql", mysqlInfo)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't open connection to postgres database")
			}

			aliases := newAliases(dsb.Alias)

			//create a query with placeholders to prepare a statement from a physical formula
			query := formulaToSQL(dsb.Filter, aliases)
			query = fmt.Sprintf("SELECT * FROM %s %s WHERE %s", tableName, dsb.Alias, query)

			stmt, err := db.Prepare(query)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't prepare db for query")
			}

			//materialize the created aliases
			execAliases, err := aliases.materializeAliases(matCtx)

			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize aliases")
			}

			return &DataSource{
				stmt:    stmt,
				aliases: execAliases,
				alias:   dsb.Alias,
				db:      db,
			}, nil
		},
		primaryKeys,
		availableFilters,
		metadata.BoundedDoesntFitInLocalStorage,
	)
}

// NewDataSourceBuilderFactoryFromConfig creates a data source builder factory using the configuration.
func NewDataSourceBuilderFactoryFromConfig(dbConfig map[string]interface{}) (physical.DataSourceBuilderFactory, error) {
	primaryKeysStrings, err := common.GetStringList(dbConfig, "primaryKeys", common.WithDefault([]string{}))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get primaryKeys")
	}
	var primaryKeys []common.VariableName
	for _, str := range primaryKeysStrings {
		primaryKeys = append(primaryKeys, common.NewVariableName(str))
	}

	return NewDataSourceBuilderFactory(primaryKeys), nil
}

func (ds *DataSource) Get(variables common.Variables) (execution.RecordStream, error) {
	values := make([]interface{}, 0)

	for i := range ds.aliases {
		expression := ds.aliases[i]

		//since we have an execution expression, then we can evaluate it given the variables
		value, err := expression.ExpressionValue(variables)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get actual value from variables")
		}

		values = append(values, value)
	}

	rows, err := ds.stmt.Query(values...)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't query statement")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get columns from rows")
	}

	return &RecordStream{
		rows:    rows,
		columns: columns,
		isDone:  false,
		alias:   ds.alias,
	}, nil

}

type RecordStream struct {
	rows    *sql.Rows
	columns []string
	isDone  bool
	alias   string
}

func (rs *RecordStream) Close() error {
	err := rs.rows.Close()
	if err != nil {
		return errors.Wrap(err, "Couldn't close underlying SQL rows")
	}

	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	if rs.isDone {
		return nil, execution.ErrEndOfStream
	}

	if !rs.rows.Next() {
		rs.isDone = true
		return nil, execution.ErrEndOfStream
	}

	cols := make([]interface{}, len(rs.columns))
	colPointers := make([]interface{}, len(cols))
	for i := range cols {
		colPointers[i] = &cols[i]
	}

	if err := rs.rows.Scan(colPointers...); err != nil {
		return nil, errors.Wrap(err, "couldn't scan row")
	}

	resultMap := make(map[common.VariableName]common.Value)

	fields := make([]common.VariableName, len(rs.columns))
	for i, columnName := range rs.columns {
		newName := common.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, columnName))
		fields[i] = newName
		resultMap[newName] = common.NormalizeType(cols[i])
	}

	return execution.NewRecord(fields, resultMap), nil
}
