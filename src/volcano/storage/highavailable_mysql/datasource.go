package highavailable_mysql

import (
	"context"
	"fmt"
	"strings"

	"backend"
	"router"
	"xcontext"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"volcano/common"
	"volcano/execution"
	"volcano/physical"
	"volcano/physical/metadata"

	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
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
	rawQuery string
	querys   []xcontext.QueryTuple
	//spanner *proxy.Spanner
	scatter *backend.Scatter
	aliases []execution.Expression
	database string
	tableName string
	alias   string
}

func calcRoute(route *router.Router, database, tableName string) ([]router.Segment, error) {
	segments, err := route.GetSegments(database, tableName, []int{})
	if err != nil {
		return nil, err
	}
	return segments, nil
}

// buildQuery used to build the QueryTuple.
func buildQuery(query string, segments []router.Segment, tableName string) []xcontext.QueryTuple {
	var querys[]xcontext.QueryTuple
	for i, segment := range segments {
		// Rewrite the shard table's name.
		backend := segment.Backend

		rang := segments[i].Range.String()
		newTable := segments[i].Table
		query := strings.Replace(query, tableName, newTable, 1)

		tuple := xcontext.QueryTuple{
			Query:   query,
			Backend: backend,
			Range:   rang,
		}
		querys = append(querys, tuple)
	}
	return querys
}

func NewDataSourceBuilderFactory(primaryKeys []common.VariableName) physical.DataSourceBuilderFactory {
	return physical.NewDataSourceBuilderFactory(
		func(ctx context.Context, matCtx *physical.MaterializationContext, dbConfig map[string]interface{}, dsb *physical.DataSourceBuilder) (execution.Node, error) {
			////Get execution configuration
			//host, port, err := common.GetIPAddress(dbConfig, "address", common.WithDefault([]interface{}{"localhost", 3306}))
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't get address")
			//}
			//user, err := common.GetString(dbConfig, "user")
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't get user")
			//}
			//databaseName, err := common.GetString(dbConfig, "databaseName")
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't get databaseName")
			//}
			//tableName, err := common.GetString(dbConfig, "tableName")
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't get tableName")
			//}
			//password, err := common.GetString(dbConfig, "password")
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't get password")
			//}
			//
			//// Build dsn
			//mysqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", "root", "123456", "127.0.0.1", 3308, dsb.DataBase)
			//
			//db, err := sql.Open("mysql", mysqlInfo)
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't open connection to postgres database")
			//}
			//
			aliases := newAliases(dsb.Alias)

			////create a query with placeholders to prepare a statement from a physical formula
			query := formulaToSQL(dsb.Filter, aliases)
			query = fmt.Sprintf("SELECT * FROM %s.%s %s WHERE %s", dsb.DataBase, dsb.Name, dsb.Alias, query)

			//stmt, err := db.Prepare(query)
			//if err != nil {
			//	return nil, errors.Wrap(err, "couldn't prepare db for query")
			//}
			//materialize the created aliases
			execAliases, err := aliases.materializeAliases(matCtx)
			if err != nil {
				return nil, errors.Wrap(err, "couldn't materialize aliases")
			}

			route := matCtx.Router
			scatter := matCtx.Scatter
			segments, err := calcRoute(route, dsb.DataBase, dsb.Name)

			querys := buildQuery(query, segments, dsb.Name)

			return &DataSource{
				rawQuery: query,
				querys:  querys,
				//spanner: matCtx.Spanner,
				scatter: scatter,
				aliases: execAliases,
				alias:   dsb.Alias,
				database: dsb.DataBase,
				tableName: dsb.Name,
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

	scatter := ds.scatter
	//sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		return nil, err
	}
	defer txn.Finish()

	// binding.
	//sessions.TxnBinding(session, txn, node, rawQuery)
	//defer sessions.TxnUnBinding(session)

	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = xcontext.ReqNormal
	reqCtx.Querys = ds.querys
	reqCtx.RawQuery = ds.rawQuery
	rowss, err := txn.ExecuteVolcanoStreamFetch(reqCtx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't query statement")
	}

	columns := rowss[0].Fields()

	return &RecordStream{
		rowss:   rowss,
		isDones: make([]int, len(rowss)),
		pos: 	 0,
		columns: columns,
		isDone:  false,
		alias:   ds.alias,
	}, nil
}

type RecordStream struct {
	rowss   []driver.Rows
	isDones []int
	pos 	int
	columns []*querypb.Field
	isDone  bool
	alias   string
}

func (rs *RecordStream) Close() error {
	for _, row := range rs.rowss {
		err := row.Close()
		if err != nil {
			return errors.Wrap(err, "Couldn't close underlying SQL rows")
		}
	}
	return nil
}

func (rs *RecordStream) Next() (*execution.Record, error) {
	//if rs.isDone {
	//	//return nil, execution.ErrEndOfStream
	//	return nil,nil
	//}
	lenRows := len(rs.rowss)

start:
	rows := rs.rowss[rs.pos]
	count := 0
	for count < 2 {
		if rs.pos >= lenRows {
			rs.pos = rs.pos % lenRows
			count++
			continue
		}

		if rs.isDones[rs.pos] == 0 {
			break
		}

		rs.pos++
	}
	if count == 2 {
		return nil, execution.ErrEndOfStream
	}

	if !rows.Next() {
		//rs.isDone = true
		//return nil, execution.ErrEndOfStream
		rs.isDones[rs.pos] = 1
		goto start
	}

	cols := make([]interface{}, len(rs.columns))
	colPointers := make([]interface{}, len(cols))
	for i := range cols {
		colPointers[i] = &cols[i]
	}

	row := make([]sqltypes.Value, len(rows.Fields()))
	row, err := rows.RowValues()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't scan row")
	}

	//if err := rs.rows.Scan(colPointers...); err != nil {
	//	return nil, errors.Wrap(err, "couldn't scan row")
	//}

	resultMap := make(map[common.VariableName]common.Value)

	fields := make([]common.VariableName, len(rs.columns))
	for i, columnName := range rs.columns {
		newName := common.NewVariableName(fmt.Sprintf("%s.%s", rs.alias, columnName.Name))
		fields[i] = newName
		resultMap[newName] = common.NormalizeType(row[i].ToNative())
	}

	return execution.NewRecord(fields, resultMap), nil
}
