// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

var _ driver.Conn = (*DoltConn)(nil)
var _ driver.Pinger = (*DoltConn)(nil)
var _ driver.ConnPrepareContext = (*DoltConn)(nil)
var _ driver.ExecerContext = (*DoltConn)(nil)
var _ driver.QueryerContext = (*DoltConn)(nil)

// globalQueryPid is a monotonically-increasing counter used to assign each query
// a unique PID, mirroring SessionManager.nextPid() in the go-mysql-server server path.
var globalQueryPid atomic.Uint64

// DoltConn is a driver.Conn implementation that represents a connection to a dolt database located on the filesystem
type DoltConn struct {
	se             *engine.SqlEngine
	gmsCtx         *gms.Context
	DataSource     *DoltDataSource
	cfg            *Config
	activeQueryCtx *gms.Context // non-nil while a query is active; always accessed under dc.Lock()
}

// Prepare packages up |query| as a *doltStmt so it can be executed. If multistatements mode
// has been enabled, then a *doltMultiStmt will be returned, capable of executing multiple statements.
func (d *DoltConn) Prepare(query string) (driver.Stmt, error) {
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
		conn:  d,
		query: query,
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

// beginQuery creates a fresh per-query context with a new unique PID and registers
// the query with the ProcessList. It enforces serial semantics by ending any
// previously active query before starting the new one. The provided ctx is used
// as the parent context so that both user cancellation and KILL commands work.
func (d *DoltConn) beginQuery(ctx context.Context, query string) (*gms.Context, error) {
	if d.gmsCtx.ProcessList == nil {
		return d.gmsCtx.WithContext(ctx), nil
	}
	// Enforce serial semantics: if a previous query's rows were not closed,
	// end that query in the processlist before starting the new one.
	if d.activeQueryCtx != nil {
		d.gmsCtx.ProcessList.EndQuery(d.activeQueryCtx)
		d.activeQueryCtx = nil
	}
	// Create a fresh per-query context with a new unique PID, mirroring
	// SessionManager.NewContextWithQuery in the server path. Use the caller's
	// ctx as the parent so that user cancellation propagates into the query.
	freshCtx := d.se.ContextFactory(
		ctx,
		gms.WithSession(d.gmsCtx.Session),
		gms.WithPid(globalQueryPid.Add(1)),
		gms.WithProcessList(d.gmsCtx.ProcessList),
	)
	queryCtx, err := freshCtx.ProcessList.BeginQuery(freshCtx, query)
	if err != nil {
		return nil, translateError(err)
	}
	d.activeQueryCtx = queryCtx
	return queryCtx, nil
}

// endQuery transitions the connection back to Sleep in the ProcessList.
func (d *DoltConn) endQuery(queryCtx *gms.Context) {
	if d.gmsCtx.ProcessList == nil {
		return
	}
	d.gmsCtx.ProcessList.EndQuery(queryCtx)
	if d.activeQueryCtx == queryCtx {
		d.activeQueryCtx = nil
	}
}

// Close releases the resources held by the DoltConn instance
func (d *DoltConn) Close() error {
	if d.gmsCtx == nil || d.gmsCtx.ProcessList == nil || d.gmsCtx.Session == nil {
		return nil
	}
	if d.activeQueryCtx != nil {
		d.gmsCtx.ProcessList.EndQuery(d.activeQueryCtx)
		d.activeQueryCtx = nil
	}
	d.gmsCtx.ProcessList.RemoveConnection(d.gmsCtx.Session.ID())
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

	queryCtx, _, iter, err := d.queryWithBindings(ctx, "begin", nil)
	if err != nil {
		return nil, err
	}
	if err := iter.Close(queryCtx); err != nil {
		return nil, translateError(err)
	}

	return &doltTx{
		ctx:  ctx,
		conn: d,
	}, nil
}

// Ping implements driver.Pinger. It verifies the connection is still alive by
// executing a lightweight query against the engine.
func (d *DoltConn) Ping(ctx context.Context) error {
	queryCtx, _, itr, err := d.queryWithBindings(ctx, "select 1", nil)
	if err != nil {
		return driver.ErrBadConn
	}
	if err := itr.Close(queryCtx); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// PrepareContext implements driver.ConnPrepareContext. The supplied context
// governs the preparation step only; the returned statement inherits the
// connection's ambient context for its executions.
func (d *DoltConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return d.Prepare(query)
}

// ExecContext implements driver.ExecerContext.
func (d *DoltConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := d.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

// QueryContext implements driver.QueryerContext.
func (d *DoltConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := d.Prepare(query)
	if err != nil {
		return nil, err
	}
	// Do not Close stmt here: the rows are still live and the stmt must not be
	// closed while they are outstanding. doltStmt holds no resources that need
	// explicit cleanup beyond what doltRows already owns, so it will be GC'd.
	return stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

func (d *DoltConn) queryWithBindings(ctx context.Context, query string, bindings map[string]sqlparser.Expr) (*gms.Context, gms.Schema, gms.RowIter, error) {
	queryCtx, err := d.beginQuery(ctx, query)
	if err != nil {
		return nil, nil, nil, err
	}
	queryCtx.SetQueryTime(time.Now())
	sch, itr, _, err := d.se.QueryWithBindings(queryCtx, query, nil, bindings, nil)
	if err != nil {
		d.endQuery(queryCtx)
		return nil, nil, nil, translateError(err)
	}
	return queryCtx, sch, &callbackOnCloseIter{
		iter: itr,
		callback: func() {
			d.endQuery(queryCtx)
		},
	}, nil
}

type callbackOnCloseIter struct {
	iter     gms.RowIter
	callback func()
}

func (c callbackOnCloseIter) Next(ctx *gms.Context) (gms.Row, error) {
	return c.iter.Next(ctx)
}

func (c callbackOnCloseIter) Close(ctx *gms.Context) error {
	err := c.iter.Close(ctx)
	c.callback()
	return err
}
