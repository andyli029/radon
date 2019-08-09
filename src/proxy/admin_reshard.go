/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"backend"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	ReshardPrefix = "radon_reshard_"
)

type Reshard struct {
	mu              sync.RWMutex
	wg              sync.WaitGroup
	log             *xlog.Log
	scatter         *backend.Scatter
	router          *router.Router
	spanner         *Spanner
	user            string
	db              string
	singleTable     string
	dstDB           string
	reshardTable    string
	tmpReshardTable string
	ticker          *time.Ticker
	handle          ReshardHandle
}

var _ ReshardHandle = &Reshard{}

type ReshardHandle interface {
	ShiftProcess() error
}

func (reshard *Reshard) ShiftProcess() error {
	return shiftTableLow(reshard.db, reshard.singleTable, reshard.dstDB, reshard.reshardTable, reshard.user, reshard.spanner)
}

func (reshard *Reshard) Callback() error {
	log := reshard.log
	log.Warning("reshard.table[%s.%s].successfully.", reshard.db, reshard.singleTable)
	return nil
}

func NewReshard(log *xlog.Log, scatter *backend.Scatter, router *router.Router,
	spanner *Spanner, user string) *Reshard {
	return &Reshard{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
		ticker:  time.NewTicker(time.Duration(time.Second * 5)),
		user:    user,
	}
}

func (reshard *Reshard) SetHandle(r ReshardHandle) {
	reshard.handle = r
}

func (reshard *Reshard) Check(db, singleTable, dstDB, dstTable string) (bool, error) {
	isSingle, err := reshard.IsSingleTable(db, singleTable)
	if err != nil {
		return false, err
	}

	if isSingle != true {
		return false, nil
	}

	err = reshard.router.CheckDatabase(dstDB)
	if err != nil {
		return false, err
	}

	// make sure the dstTable is not exist to the shift.
	_, err = reshard.router.TableConfig(dstDB, dstTable)
	if err != nil {
		return true, err
	}

	return false, nil
}

func (reshard *Reshard) IsSingleTable(db, singleTable string) (bool, error) {
	table, err := reshard.router.TableConfig(db, singleTable)
	if err != nil {
		return false, err
	}

	if table.ShardType == "SINGLE" {
		return true, nil
	}
	return false, nil
}

func (reshard *Reshard) ReShardNormal(db, singleTable, dstDB, dstTable string) (*sqltypes.Result, error) {
	log := reshard.log
	qr := &sqltypes.Result{}

	if ok, err := reshard.Check(db, singleTable, dstDB, dstTable); ok != true ||
		err == nil {
		log.Error("reshard.check[%s.%s->%s.%s].is.not.ok:%v", db, singleTable, dstDB, dstTable, err)
		err := fmt.Sprintf("reshard.check[%s.%s->%s.%s].is.not.ok:%v", db, singleTable, dstDB, dstTable, err)
		return qr, errors.New(err)
	}
	reshard.db = db
	reshard.singleTable = singleTable
	reshard.dstDB = dstDB
	reshard.reshardTable = dstTable

	//todo: shift data from singleTable to the reshard table
	if err := reshard.shiftTable(reshard.user); err != nil {
		log.Error("reshard.table[%s.%s].create.tmp.reshard.table:%v", db, singleTable, err)
		return qr, err
	}

	return qr, nil
}

func (reshard *Reshard) shiftTable(user string) error {
	var wg sync.WaitGroup

	oneshift := func(db, srcTable, dstDB, dstTable string, user string, spanner *Spanner) error {
		defer wg.Done()

		err := reshard.handle.ShiftProcess()
		if err != nil {
			return err
		}

		reshard.Callback()

		return nil
	}

	wg.Add(1)
	go oneshift(reshard.db, reshard.singleTable, reshard.dstDB, reshard.reshardTable, user, reshard.spanner)

	//wg.Wait()

	return nil
}
