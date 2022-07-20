package embedded

import (
	"database/sql/driver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

type DoltConn struct {
	se     *engine.SqlEngine
	SqlCtx *gms.Context
}

func (d *DoltConn) Prepare(query string) (driver.Stmt, error) {
	_, err := d.se.GetUnderlyingEngine().PrepareQuery(d.SqlCtx, query)
	if err != nil {
		return nil, err
	}

	return &DoltStmt{
		QueryStr: query,
		se:       d.se,
		SqlCtx:   d.SqlCtx,
	}, nil
}

func (d *DoltConn) Close() error {
	return d.se.Close()
}

func (d *DoltConn) Begin() (driver.Tx, error) {
	_, _, err := d.se.Query(d.SqlCtx, "BEGIN;")
	if err != nil {
		return nil, err
	}

	return &DoltTx{
		se:     d.se,
		sqlCtx: d.SqlCtx,
	}, nil
}
