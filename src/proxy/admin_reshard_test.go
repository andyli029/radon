/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"backend"
	"fakedb"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	showBinlogFormat = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Variable_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Value",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("binlog_format")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("ROW")),
			},
		},
	}
)

type TestHandler struct {
	mu      sync.RWMutex
	address string
}

func (th *TestHandler) ShiftProcess() error {
	var err error

	client, err := driver.NewConn("mock", "mock", th.address, "", "utf8")
	querys := []string{
		"create table test.tmp_reshard_a(i int primary key)",
	}
	for _, query := range querys {
		_, err = client.FetchAll(query, -1)
	}
	//time.Sleep(1 *time.Second)
	return err
}

func TestReshardMockShiftLow(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	scatter := proxy.Scatter()
	router := proxy.Router()
	spanner := proxy.Spanner()
	address := proxy.Address()

	fakedb1, _, _, _, cleanup1 := backend.MockTxnMgr(log, 2)
	defer cleanup1()
	//backend1 := addrs[1]

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("drop table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", showTablesResult3)
		//fakedbs.AddQueryPattern("show .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show .*", showCreateTableResult)
		fakedbs.AddQuery("SHOW GLOBAL VARIABLES LIKE \"binlog_format\"", showBinlogFormat)
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		querys := []string{
			"create table test.a(i int primary key) single",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		querys := []string{
			"create table test.s(i int primary key)",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// Insert.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.a (id, b) values(1),(3)"
		fakedb1.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// radon reshard failed.
	{
		query := "radon reshard test1.s to test1.b"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		reshard := NewReshard(log, scatter, router, spanner, "mock")
		th := &TestHandler{address: address}
		reshard.SetHandle(th)

		_, err = reshard.ReShardNormal("test1", "s", "test1", "b")
		assert.NotNil(t, err)
	}

	// radon reshard failed.
	{
		query := "radon reshard test.s to test1.b"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		reshard := NewReshard(log, scatter, router, spanner, "mock")
		th := &TestHandler{address: address}
		reshard.SetHandle(th)

		_, err = reshard.ReShardNormal("test", "s", "test1", "b")
		assert.NotNil(t, err)
	}

	// radon reshard failed.
	{
		query := "radon reshard test.a to test1.b"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		reshard := NewReshard(log, scatter, router, spanner, "mock")
		th := &TestHandler{address: address}
		reshard.SetHandle(th)

		_, err = reshard.ReShardNormal("test", "a", "test1", "b")
		assert.NotNil(t, err)
	}

	// radon reshard successfull.
	{
		query := "radon reshard test.a to test.b"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		reshard := NewReshard(log, scatter, router, spanner, "mock")
		th := &TestHandler{address: address}
		reshard.SetHandle(th)

		_, err = reshard.ReShardNormal("test", "a", "test", "b")
		assert.Nil(t, err)
	}
}
