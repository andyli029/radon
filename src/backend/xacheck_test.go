/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"config"
	"fakedb"
	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqldb"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	xaRecoverResult1 = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "formatID",
				Type: querypb.Type_INT64,
			},
			{
				Name: "gtrid_length",
				Type: querypb.Type_INT64,
			},
			{
				Name: "bqual_length",
				Type: querypb.Type_INT64,
			},
			{
				Name: "data",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("21")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("RXID-20180903103145-1")),
			},
		},
	}

	xaRecoverResult2 = &sqltypes.Result{
		RowsAffected: 0,
		Fields: []*querypb.Field{
			{
				Name: "formatID",
				Type: querypb.Type_INT64,
			},
			{
				Name: "gtrid_length",
				Type: querypb.Type_INT64,
			},
			{
				Name: "bqual_length",
				Type: querypb.Type_INT64,
			},
			{
				Name: "data",
				Type: querypb.Type_VARCHAR,
			},
		},
	}
)

func TestWriteXaCommitErrorLogsAddXidDuplicate(t *testing.T) {

	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	scatter, _, cleanup := MockScatter(log, 2)
	defer cleanup()

	scatter.Init(MockScatterDefault(log))
	txn1, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn1.Finish()

	txn1.xid = "RXID-20180903103145-1"
	backend := "backend0"

	err = scatter.txnMgr.xaCheck.WriteXaCommitErrLog(txn1, backend)
	assert.Nil(t, err)
	//scatter.txnMgr.xaCheck.RemoveXaRedoLogs()
	txn2, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn2.Finish()

	txn2.xid = "RXID-20180903103145-1"
	backend = "backend0"

	err = scatter.txnMgr.xaCheck.WriteXaCommitErrLog(txn2, backend)
	assert.NotNil(t, err)
}

func TestReadXaCommitErrorLogsWithoutBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	data := `{
    "xacommit-errs": [
        {
            "time": "20180903103145",
            "xaid": "RXID-20180903103145-1",
            "state": "commit"
        }
    ]
}`

	dir := fakedb.GetTmpDir("/tmp", "xacheck", log)
	file := path.Join(dir, xacheckJSONFile)
	ioutil.WriteFile(file, []byte(data), 0644)
	defer os.RemoveAll(file)

	scatter := NewScatter(log, "")
	scatter.Init(MockScatterDefault2(dir))

	time.Sleep(1 * time.Second)

	scatter.txnMgr.xaCheck.Close()
}

func TestTxnTwoPCExecuteCommitError(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	scatter, fakedb1, cleanup1 := MockScatter(log, 2)
	defer cleanup1()

	scatter.Init(MockScatterDefault(log))
	var backend [2]string
	var i int
	for k := range scatter.backends {
		backend[i] = k
		i++
	}

	querys1 := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: backend[0]},
		xcontext.QueryTuple{Query: "update", Backend: backend[1]},
	}

	fakedb1.AddQuery(querys1[0].Query, result1)
	fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc1 := func(txn *Txn) {
		fakedb1.ResetAll()
		fakedb1.AddQuery(querys1[0].Query, result1)
		fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)
		fakedb1.AddQueryPattern("XA .*", result1)
	}

	// XA COMMIT error.
	{
		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()

		resetFunc1(txn)
		fakedb1.AddQueryErrorPattern("XA COMMIT .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn.Execute(rctx)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
		time.Sleep(2 * time.Second)
	}

	_, err := os.Stat(scatter.txnMgr.xaCheck.GetXaCheckFile())
	assert.Nil(t, err)

	err = scatter.txnMgr.xaCheck.RemoveXaCommitErrLogs()
	assert.Nil(t, err)
}

// the command XA RECOVER is not reponsed
func TestXaCheckFailed(t *testing.T) {

	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	scatter, fakedb1, cleanup1 := MockScatter(log, 2)
	defer cleanup1()

	scatter.Init(MockScatterDefault(log))
	var backend [2]string
	var i int
	for k := range scatter.backends {
		backend[i] = k
		i++
	}

	querys1 := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: backend[0]},
		xcontext.QueryTuple{Query: "update", Backend: backend[1]},
	}

	fakedb1.AddQuery(querys1[0].Query, result1)
	fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc1 := func(txn *Txn) {
		fakedb1.ResetAll()
		fakedb1.AddQuery(querys1[0].Query, result1)
		fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)
		fakedb1.AddQueryPattern("XA .*", result1)
	}

	// XA COMMIT error.
	{
		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()

		resetFunc1(txn)
		fakedb1.AddQueryErrorPattern("XA COMMIT .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn.Execute(rctx)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
		//time.Sleep(2 * time.Second)
	}

	// XA COMMIT ok.
	{
		txn2, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn2.Finish()

		resetFunc1(txn2)
		fakedb1.AddQuery("XA COMMIT .*", result1)

		err = txn2.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn2.Execute(rctx)
		assert.Nil(t, err)
		err = txn2.Commit()
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
	}

	// the xacheck is stil exit.
	_, err := os.Stat(scatter.txnMgr.xaCheck.GetXaCheckFile())
	assert.Nil(t, err)

}

func TestXaCheckFromFileOK(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	data := `{
    "xacommit-errs": [
        {
            "time": "20180903103145",
            "xaid": "RXID-20180903103145-1",
            "state": "commit"
        }
    ]
}`
	dir := fakedb.GetTmpDir("/tmp", "xacheck", log)
	file := path.Join(dir, xacheckJSONFile)
	ioutil.WriteFile(file, []byte(data), 0644)
	defer os.RemoveAll(file)

	// the new
	scatter, fakedb1, cleanup1 := MockScatter(log, 2)
	defer cleanup1()

	scatter.Init(MockScatterDefault2(dir))
	var backend [2]string
	var i int
	for k := range scatter.backends {
		backend[i] = k
		i++
	}

	querys1 := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: backend[0]},
		xcontext.QueryTuple{Query: "update", Backend: backend[1]},
	}

	fakedb1.AddQuery(querys1[0].Query, result1)
	fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc1 := func(txn *Txn) {
		fakedb1.ResetAll()
		fakedb1.AddQuery(querys1[0].Query, result1)
		fakedb1.AddQueryDelay(querys1[1].Query, result2, 100)
		fakedb1.AddQueryPattern("XA .*", result1)
	}

	// XA RECOVER ok
	// XA COMMIT ok.
	{
		txn2, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn2.Finish()

		resetFunc1(txn2)
		fakedb1.AddQuery("XA RECOVER", xaRecoverResult1)
		fakedb1.AddQuery("XA COMMIT .*", result1)

		err = txn2.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn2.Execute(rctx)
		assert.Nil(t, err)
		err = txn2.Commit()
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
	}

	// XA RECOVER and return Empty set
	// XA COMMIT ok.
	{
		txn2, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn2.Finish()

		resetFunc1(txn2)
		fakedb1.AddQuery("XA RECOVER", xaRecoverResult2)
		fakedb1.AddQuery("XA COMMIT .*", result1)

		err = txn2.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys1,
		}
		_, err = txn2.Execute(rctx)
		assert.Nil(t, err)
		err = txn2.Commit()
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
	}

	//scatter.txnMgr.xaCheck.Close()
}

func TestLoadXaCommitRrrLogs(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockScatterDefault(log))
	defer os.RemoveAll(xaChecker.dir)
	err := xaChecker.Init()
	assert.Nil(t, err)

	err = xaChecker.LoadXaCommitErrLogs()
	assert.Nil(t, err)

	xaChecker.Close()
}

func TestReadXaCommitRrrLogs1(t *testing.T) {
	defer leaktest.Check(t)()

	data := `{
    "xacommit-errs": [
        {
            "time": "20180903103145",
            "xaid": "RXID-20180903103145-1",
            "state": "rollback"
        }
    ]
}`

	MockXaredologs := []*XaCommitErr{
		&XaCommitErr{
			Time:     "20180903103145",
			Xaid:     "RXID-20180903103145-1",
			State:    txnXACommitErrStateRollback,
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockScatterDefault(log))
	err := xaChecker.Init()
	assert.Nil(t, err)

	xaCommitErrLogs, err := xaChecker.ReadXaCommitErrLogs(string(data))
	assert.Nil(t, err)
	want := &XaCommitErrs{Logs: MockXaredologs}
	got := xaCommitErrLogs
	assert.Equal(t, want, got)
	xaChecker.Close()
	xaChecker.RemoveXaCommitErrLogs()
}

func TestReadXaCommitRrrLogsError2(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	xaChecker := NewXaCheck(scatter, MockScatterDefault(log))

	err := xaChecker.Init()
	assert.Nil(t, err)

	data1 := `{
    "xacommit-errs": [
		2
    ]
}`

	file := path.Join(xaChecker.dir, xacheckJSONFile)
	ioutil.WriteFile(file, []byte(data1), 0644)
	defer os.RemoveAll(file)

	err = xaChecker.Init()
	assert.NotNil(t, err)

	xaChecker.Close()
}

func TestReadXaCommitRrrLogsInit(t *testing.T) {
	defer leaktest.Check(t)()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "")

	MockXaredologs := []*XaCommitErr{
		&XaCommitErr{
			Time:     "20180903103145",
			Xaid:     "RXID-20180903103145-1",
			State:    txnXACommitErrStateRollback,
		},
	}
	dir := fakedb.GetTmpDir("/tmp", "xacheck", log)
	file := path.Join(dir, xacheckJSONFile)
	config.WriteConfig(file, &XaCommitErrs{Logs: MockXaredologs})
	defer os.RemoveAll(file)

	xaChecker := NewXaCheck(scatter, MockScatterDefault2(dir))

	err := xaChecker.Init()
	assert.Nil(t, err)
	xaChecker.Close()
}
