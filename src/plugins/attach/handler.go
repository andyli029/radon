/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package attach

import (
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

type AttachHandler interface {
	Init() error
	Attach(radon *sqlparser.Radon) (*sqltypes.Result, error)
	Detach(attachName string) (*sqltypes.Result, error)
	ListAttach() (*sqltypes.Result, error)
}
