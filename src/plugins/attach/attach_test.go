/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package attach

import (
	"errors"
	"testing"

	"backend"
	"router"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"fmt"
)

var (
	showDatabasesResult = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sys")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("db")),
			},
		},
	}

	showTablesResult = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
			},
		},
	}

	showTablesResult2 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
			},
		},
	}

	showTablesResult3 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("b")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("A")),
			},
		},
	}

	showCreateTableResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR,
					[]byte("CREATE TABLE `a` (`i` int(11) NOT NULL, PRIMARY KEY (`i`)) ENGINE=InnoDB DEFAULT CHARSET=utf8")),
			},
		},
	}
)

func TestAttachAndListAndDetach(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, fakedb, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()

	fakedb1, _, _, addrs, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb1.AddQueryPattern("drop database .*", &sqltypes.Result{}) // attach backend
		fakedb1.AddQueryPattern("show create .*", showCreateTableResult) // attach backend
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})     // attach backend
		fakedb1.AddQuery("show databases", showDatabasesResult)     // attach backend
		fakedb1.AddQueryPattern("show tables .*", showTablesResult) // attach backend
	}

	// Router.
	router, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, router)

	query := fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.Nil(t, err)

	query = fmt.Sprintf("radon attachlist")
	node, err = sqlparser.Parse(query)
	assert.Nil(t, err)
	_, err = handler.ListAttach()
	assert.Nil(t, err)

	query = fmt.Sprintf("radon detach('attach1')")
	node, err = sqlparser.Parse(query)
	assert.Nil(t, err)
	_, err = handler.Detach(backend1)
	assert.Nil(t, err)
}

func TestAttachErrorParams(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, _, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()
	// Router.
	router, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, router)

	query := fmt.Sprintf("radon attach('attach1');")
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}

func TestAttachErrorDuplicateBackend(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, _, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()
	// Router.
	router, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, router)

	query := fmt.Sprintf("radon attach('backend1', 'mock', 'pwd')")
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}

func TestAttachErrorShow(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, fakedb, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()

	fakedb1, _, _, addrs, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	backend1 := addrs[1]

	// Router.
	router, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, router)

	// show databases error.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb1.AddQueryError("show databases", errors.New("show.databases.error"))    // attach backend
		fakedb1.AddQueryPattern("show tables .*", showTablesResult) // attach backend
	}

	query := fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)

	// show tables error.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb1.AddQuery("show databases", showDatabasesResult)     // attach backend
		fakedb1.AddQueryErrorPattern("show tables .*", errors.New("show.tables.error")) // attach backend
	}

	query = fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err = sqlparser.Parse(query)
	assert.Nil(t, err)
	attach = node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}

func TestAttachErrorRouteCreateTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, fakedb, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()

	fakedb1, _, _, addrs, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb1.AddQueryPattern("drop database .*", &sqltypes.Result{}) // attach backend
		fakedb1.AddQueryPattern("show create .*", showCreateTableResult) // attach backend
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})     // attach backend
		fakedb1.AddQuery("show databases", showDatabasesResult)     // attach backend
		fakedb1.AddQueryPattern("show tables .*", showTablesResult) // attach backend
	}

	// Router.
	router, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, router)

	query := fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.Nil(t, err)

	backend2 := addrs[2]
	query = fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend2)
	node, err = sqlparser.Parse(query)
	assert.Nil(t, err)
	attach = node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}

func TestAttachErrorDeferCleanTableRouter(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, fakedb, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()

	fakedb1, _, _, addrs, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb1.AddQueryPattern("drop database .*", &sqltypes.Result{}) // attach backend
		fakedb1.AddQueryPattern("show create .*", showCreateTableResult) // attach backend
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})     // attach backend
		fakedb1.AddQuery("show databases", showDatabasesResult)     // attach backend
		fakedb1.AddQueryPattern("show tables .*", showTablesResult3) // attach backend
	}

	// Router.
	route, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	database := "test"
	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	handler := NewAttach(log, scatter, route)

	query := fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}

func TestAttachErrorDeferDBRouter(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// radon attach will flush file, we need clean the tmpdir when test finish
	scatter, fakedb, cleanup := backend.MockScatterTmpDir(log, 2)
	defer cleanup()

	fakedb1, _, _, addrs, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb.AddQueryPattern("create .*", &sqltypes.Result{})     // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb.AddQueryPattern("drop database .*", &sqltypes.Result{}) // normal backend
		fakedb1.AddQueryPattern("drop database .*", &sqltypes.Result{}) // attach backend
		fakedb1.AddQueryPattern("show create .*", showCreateTableResult) // attach backend
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})     // attach backend
		fakedb1.AddQuery("show databases", showDatabasesResult)     // attach backend
		fakedb1.AddQueryPattern("show tables .*", showTablesResult2) // attach backend
	}

	// Router.
	route, cleanup2 := router.MockNewRouter(log)
	defer cleanup2()

	handler := NewAttach(log, scatter, route)

	query := fmt.Sprintf("radon attach('%s', 'mock', 'pwd')", backend1)
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	attach := node.(*sqlparser.Radon)
	_, err = handler.Attach(attach)
	assert.NotNil(t, err)
}
