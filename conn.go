package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

var _ driver.Conn = (*DoltConn)(nil)

// DoltConn is a driver.Conn implementation that represents a connection to a dolt database located on the filesystem
type DoltConn struct {
	se         *engine.SqlEngine
	gmsCtx     *gms.Context
	DataSource *DoltDataSource
	cfg        *Config
}

// Prepare packages up |query| as a *doltStmt so it can be executed. If multistatements mode
// has been enabled, then a *doltMultiStmt will be returned, capable of executing multiple statements.
func (d *DoltConn) Prepare(query string) (driver.Stmt, error) {
	// Reuse the same ctx instance, but update the QueryTime to the current time.
	// Statements are executed serially on a connection, so it's safe to reuse
	// the same ctx instance and update the time.
	d.gmsCtx.SetQueryTime(time.Now())

	multi := false
	if d.cfg != nil {
		multi = d.cfg.MultiStatements
	} else if d.DataSource != nil {
		multi = d.DataSource.ParamIsTrue(MultiStatementsParam)
	}

	if multi {
		return d.prepareMultiStatement(query)
	} else {
		return d.prepareSingleStatement(query)
	}
}

// prepareSingleStatement creates a doltStmt from |query|.
func (d *DoltConn) prepareSingleStatement(query string) (*doltStmt, error) {
	return &doltStmt{
		query:  query,
		se:     d.se,
		gmsCtx: d.gmsCtx,
	}, nil
}

// prepareMultiStatement creates a doltStmt from each individual statement in |query|.
func (d *DoltConn) prepareMultiStatement(query string) (*doltMultiStmt, error) {
	var doltMultiStmt doltMultiStmt
	scanner := gms.NewMysqlParser()

	remainder := query
	var err error
	for remainder != "" {
		_, query, remainder, err = scanner.Parse(d.gmsCtx, remainder, true)
		if err == sqlparser.ErrEmpty {
			// Skip over any empty statements
			continue
		} else if err != nil {
			return nil, translateError(err)
		}

		doltStmt, err := d.prepareSingleStatement(query)
		if err != nil {
			return nil, translateError(err)
		}
		doltMultiStmt.stmts = append(doltMultiStmt.stmts, doltStmt)
	}

	return &doltMultiStmt, nil
}

// Close releases the resources held by the DoltConn instance
func (d *DoltConn) Close() error {
	return nil
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

	_, _, _, err := d.se.Query(d.gmsCtx, "BEGIN;")
	if err != nil {
		return nil, translateError(err)
	}

	return &doltTx{
		se:     d.se,
		gmsCtx: d.gmsCtx,
	}, nil
}
