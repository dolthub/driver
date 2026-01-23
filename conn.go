package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

var _ driver.Conn = (*DoltConn)(nil)

// DoltConn is a driver.Conn implementation that represents a connection to a dolt database located on the filesystem
type DoltConn struct {
	mu sync.Mutex

	se         *engine.SqlEngine
	gmsCtx     *gms.Context
	DataSource *DoltDataSource

	// retryPolicy is parsed from DSN parameters and used by retry logic.
	retryPolicy RetryPolicy

	// txDepth tracks whether a SQL transaction is active for this connection.
	// Retry logic that reopens the engine is not safe while a transaction is open.
	txDepth int
}

func (d *DoltConn) getEngineAndContext() (*engine.SqlEngine, *gms.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.se, d.gmsCtx
}

func (d *DoltConn) getRetryPolicy() RetryPolicy {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.retryPolicy
}

func (d *DoltConn) inTransaction() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.txDepth > 0
}

func (d *DoltConn) beginTx() {
	d.mu.Lock()
	d.txDepth++
	d.mu.Unlock()
}

func (d *DoltConn) endTx() {
	d.mu.Lock()
	if d.txDepth > 0 {
		d.txDepth--
	}
	d.mu.Unlock()
}

// reopenEngine closes the current engine (if any) and rebuilds a new engine+context from DataSource.
// Caller should only use this as part of retry logic.
func (d *DoltConn) reopenEngine(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.DataSource == nil {
		return fmt.Errorf("cannot reopen engine: missing DataSource")
	}
	if d.txDepth > 0 {
		return fmt.Errorf("cannot reopen engine while a transaction is active")
	}

	// Close previous engine (best-effort).
	if d.se != nil {
		_ = d.se.Close()
	}

	se, gmsCtx, rp, err := openEmbeddedEngine(ctx, d.DataSource)
	if err != nil {
		return err
	}
	d.se = se
	d.gmsCtx = gmsCtx
	d.retryPolicy = rp
	return nil
}

// Prepare packages up |query| as a *doltStmt so it can be executed. If multistatements mode
// has been enabled, then a *doltMultiStmt will be returned, capable of executing multiple statements.
func (d *DoltConn) Prepare(query string) (driver.Stmt, error) {
	// Reuse the same ctx instance, but update the QueryTime to the current time.
	// Statements are executed serially on a connection, so it's safe to reuse
	// the same ctx instance and update the time.
	_, gmsCtx := d.getEngineAndContext()
	if gmsCtx != nil {
		gmsCtx.SetQueryTime(time.Now())
	}

	if d.DataSource.ParamIsTrue(MultiStatementsParam) {
		return d.prepareMultiStatement(query)
	} else {
		return d.prepareSingleStatement(query)
	}
}

// prepareSingleStatement creates a doltStmt from |query|.
func (d *DoltConn) prepareSingleStatement(query string) (*doltStmt, error) {
	return &doltStmt{
		query: query,
		conn:  d,
	}, nil
}

// prepareMultiStatement creates a doltStmt from each individual statement in |query|.
func (d *DoltConn) prepareMultiStatement(query string) (*doltMultiStmt, error) {
	var doltMultiStmt doltMultiStmt
	scanner := gms.NewMysqlParser()

	remainder := query
	var err error
	for remainder != "" {
		_, gmsCtx := d.getEngineAndContext()
		_, query, remainder, err = scanner.Parse(gmsCtx, remainder, true)
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
	d.mu.Lock()
	se := d.se
	d.se = nil
	d.gmsCtx = nil
	d.mu.Unlock()

	if se == nil {
		return nil
	}

	err := se.Close()
	if err != context.Canceled {
		return err
	}

	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Use BeginTx instead
func (d *DoltConn) Begin() (driver.Tx, error) {
	_, gmsCtx := d.getEngineAndContext()
	return d.BeginTx(gmsCtx, driver.TxOptions{
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

	se, gmsCtx := d.getEngineAndContext()
	_, _, _, err := se.Query(gmsCtx, "BEGIN;")
	if err != nil {
		return nil, translateError(err)
	}
	d.beginTx()

	return &doltTx{
		conn: d,
	}, nil
}
