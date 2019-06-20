/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleRadon used to handle the radon attach/detach/list_attach command.
func (spanner *Spanner) handleRadon(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	attachHandler := spanner.plugins.PlugAttach()
	var err error
	var qr *sqltypes.Result
	log := spanner.log

	snode := node.(*sqlparser.Radon)
	row := snode.Row
	var attachName string

	if row != nil {
		if len(row) != 1 && len(row) != 3 {
			return nil, errors.Errorf("spanner.query.execute.radon.%s.error,", snode.Action)
		}

		if len(row) == 1 {
			val, _ := row[0].(*sqlparser.SQLVal)
			attachName = common.BytesToString(val.Val)
		}
	}

	switch snode.Action {
	case sqlparser.AttachStr:
		qr, err = attachHandler.Attach(snode)
	case sqlparser.DetachStr:
		qr, err = attachHandler.Detach(attachName)
	case sqlparser.AttachListStr:
		qr, err = attachHandler.ListAttach()
	}
	if err != nil {
		log.Error("proxy.query.multistmt.txn.[%s].error:%s", query, err)
	}
	return qr, err
}
