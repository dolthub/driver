package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Conn = (*DoltConn)(nil)

// DoltConn is a driver.Conn implementation that represents a connection to a dolt database located on the filesystem
type DoltConn struct {
	se         *engine.SqlEngine
	gmsCtx     *gms.Context
	DataSource *DoltDataSource
}

// Prepare returns a prepared statement, bound to this connection.
func (d *DoltConn) Prepare(query string) (driver.Stmt, error) {
	_, err := d.se.GetUnderlyingEngine().PrepareQuery(d.gmsCtx, query)
	if err != nil {
		return nil, err
	}

	return &doltStmt{
		query:  query,
		se:     d.se,
		gmsCtx: d.gmsCtx,
	}, nil
}

// Close releases the resources held by the DoltConn instance
func (d *DoltConn) Close() error {
	return d.se.Close()
}

// Begin starts and returns a new transaction.
//
// Deprecated: Use BeginTx instead
func (d *DoltConn) Begin() (driver.Tx, error) {
	return d.BeginTx(d.gmsCtx, driver.TxOptions{
		Isolation: driver.IsolationLevel(sql.LevelSerializable),
		ReadOnly:  false,
	})
}

// BeginTx starts and returns a new transaction.  If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
func (d *DoltConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) && opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("isolation level not supported '%d'", opts.Isolation)
	}

	_, _, err := d.se.Query(d.gmsCtx, "BEGIN;")
	if err != nil {
		return nil, err
	}

	return &doltTx{
		se:     d.se,
		gmsCtx: d.gmsCtx,
	}, nil
}
