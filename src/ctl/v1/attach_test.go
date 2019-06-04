/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"errors"
	"testing"

	"backend"
	"proxy"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
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
)

func TestCtlV1AttachAddError1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// attachParams is nil error
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", nil))
		recorded.CodeIs(500)
	}

	// duplicate backend1
	{
		fakedb1, _, _, _, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()

		// fakedbs.
		{
			fakedb1.ResetAll()
		}

		p1 := &backendParams{
			Name:           "backend1",
			Address:        "192.168.0.1:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}
}

func TestCtlV1AttachAddError2(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// error
	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQueryError("show databases", errors.New("mock.stream.query.error"))
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}

	// error
	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.ResetAll()
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryErrorPattern("create .*", errors.New("mock.stream.query.error"))
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}
}

func TestCtlV1AttachAddError3(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(200)

		backend2 := addrs[2]
		p2 := &backendParams{
			Name:           backend2,
			Address:        backend2,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded1 := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p2))
		recorded1.CodeIs(500)
	}
}

func TestCtlV1AttachAddError4(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	route, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)

	api.SetApp(route)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult2)
			fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.ResetAll()
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryError("create database .*", errors.New("mock.stream.query.error"))
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}
}

func TestCtlV1AttachAddError5(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	route, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)

	api.SetApp(route)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		{
			fakedb1.ResetAll()
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryError("create database .*", errors.New("mock.stream.query.error"))
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}
}

func TestCtlV1AttachAddError6(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
		}

		p1 := &backendParams{
			Name:           "backend1",
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(500)
	}
}

func TestCtlV1AttachAddErrorDefer(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(200)
	}
}

func TestCtlV1AttachAdd(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/attach", AddAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
		defer cleanup()
		backend1 := addrs[1]

		// fakedbs.
		{
			fakedb1.AddQuery("show databases", showDatabasesResult)
			fakedb1.AddQueryPattern("show tables .*", showTablesResult)
			fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
		}

		p1 := &backendParams{
			Name:           backend1,
			Address:        backend1,
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/attach", p1))
		recorded.CodeIs(200)
	}
}

/*
func TestCtlV1AttachAddInitBackend(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
	}

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backend", AddBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
	defer cleanup()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
	}

	p1 := &backendParams{
		Name:           backend1,
		Address:        backend1,
		User:           "mock",
		Password:       "pwd",
		MaxConnections: 1024,
	}
	recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p1))
	recorded.CodeIs(200)

	// fakedbs.
	{
		fakedb1.ResetAll()
		fakedb1.AddQueryErrorPattern("create .*", errors.New("mock.execute.error"))
	}

	backend2 := addrs[2]
	p2 := &backendParams{
		Name:           backend2,
		Address:        backend2,
		User:           "mock",
		Password:       "pwd",
		MaxConnections: 1024,
	}
	recorded = test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p2))
	recorded.CodeIs(500)
}
*/

func TestCtlV1AttachRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/attach/:name", RemoveAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/attach/backend1", nil))
		recorded.CodeIs(200)
	}
}

func TestCtlV1AttachRemoveError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/backend/:name", RemoveAttachHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 404.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/backend/xx", nil))
		recorded.CodeIs(500)
	}
}