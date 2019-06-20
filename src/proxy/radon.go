/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"proxy/admin"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleRadon used to handle the radon attach/detach/list_attach command.
func (spanner *Spanner) handleRadon(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	log := spanner.log
	attach := admin.NewAttach(log, spanner.scatter, spanner.router)

	snode := node.(*sqlparser.Radon)
	row := snode.Row
	var attachName string

	if row != nil {
		if len(row) != admin.DetachParamsCount && len(row) != admin.AttachParamsCount {
			return nil, errors.Errorf("spanner.query.execute.radon.%s.error,", snode.Action)
		}

		if len(row) == admin.DetachParamsCount {
			val, _ := row[0].(*sqlparser.SQLVal)
			attachName = common.BytesToString(val.Val)
		}
	}

	switch snode.Action {
	case sqlparser.AttachStr:
		qr, err = attach.Attach(snode)
	case sqlparser.DetachStr:
		qr, err = attach.Detach(attachName)
	case sqlparser.AttachListStr:
		qr, err = attach.ListAttach()
	}
	if err != nil {
		log.Error("proxy.query.multistmt.txn.[%s].error:%s", query, err)
	}
	return qr, err
}
