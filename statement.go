package embedded

import (
	"database/sql/driver"
	"strconv"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// doltMultiStmt represents a collection of statements to be executed against a
// Dolt database.
type doltMultiStmt struct {
	stmts []*doltStmt
}

var _ driver.Stmt = (*doltMultiStmt)(nil)

func (d doltMultiStmt) Close() error {
	var retErr error
	for _, stmt := range d.stmts {
		if err := stmt.Close(); err != nil {
			retErr = err
		}
	}

	return retErr
}

func (d doltMultiStmt) NumInput() int {
	return -1
}

func (d doltMultiStmt) Exec(args []driver.Value) (result driver.Result, retErr error) {
	for _, stmt := range d.stmts {
		var err error
		result, err = stmt.Exec(args)
		if err != nil && retErr == nil {
			// If any error occurs, record the first error, but continue executing all statements
			retErr = err
		}
	}

	// Return the first error encountered, if there was one
	if retErr != nil {
		return nil, retErr
	}

	// Otherwise, return the last result, to match the MySQL driver's behavior
	return result, nil
}

func (d doltMultiStmt) Query(args []driver.Value) (driver.Rows, error) {
	var multiResultSet doltMultiRows
	for _, stmt := range d.stmts {
		rows, err := stmt.Query(args)
		if err != nil {
			// To match the MySQL driver's behavior, we attempt to execute all statements in a multi-statement
			// query, even if some statements fail. If the first statement errors out, then we return that error,
			// otherwise we save the error from any statements, so that they can be returned from NextResultSet()
			// with the caller requests that result set.
			rows = &doltRows{err: err}
		}
		multiResultSet.rowSets = append(multiResultSet.rowSets, rows.(*doltRows))
	}

	if multiResultSet.rowSets[0].err != nil {
		return nil, multiResultSet.rowSets[0].err
	} else {
		return &multiResultSet, nil
	}
}

// doltStmt represents a single statement to be executed against a Dolt database.
type doltStmt struct {
	se     *engine.SqlEngine
	gmsCtx *gms.Context
	query  string
}

var _ driver.Stmt = (*doltStmt)(nil)

// Close closes the statement.
func (stmt *doltStmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (stmt *doltStmt) NumInput() int {
	return -1
}

func argsToBindings(args []driver.Value) (map[string]*querypb.BindVariable, error) {
	bindings := make(map[string]*querypb.BindVariable)
	var err error
	for i := range args {
		vIdx := "v" + strconv.FormatInt(int64(i+1), 10)
		bindings[vIdx], err = sqltypes.BuildBindVariable(args[i])
		if err != nil {
			return nil, err
		}
	}

	return bindings, nil
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (stmt *doltStmt) Exec(args []driver.Value) (driver.Result, error) {
	sch, itr, err := stmt.execWithArgs(args)
	if err != nil {
		return nil, translateError(err)
	}

	res := newResult(stmt.gmsCtx, sch, itr)
	if res.err != nil {
		return nil, res.err
	}

	return res, nil
}

func (stmt *doltStmt) execWithArgs(args []driver.Value) (gms.Schema, gms.RowIter, error) {
	bindings, err := argsToBindings(args)
	if err != nil {
		return nil, nil, err
	}

	sch, itr, err := stmt.se.GetUnderlyingEngine().QueryWithBindings(stmt.gmsCtx, stmt.query, nil, bindings)
	if err != nil {
		return nil, nil, err
	}
	return sch, itr, nil
}

// Query executes a query that may return rows, such as a SELECT
func (stmt *doltStmt) Query(args []driver.Value) (driver.Rows, error) {
	var sch gms.Schema
	var rowIter gms.RowIter
	var err error

	if len(args) != 0 {
		sch, rowIter, err = stmt.execWithArgs(args)
	} else {
		sch, rowIter, err = stmt.se.Query(stmt.gmsCtx, stmt.query)
	}
	if err != nil {
		return nil, translateError(err)
	}

	// Wrap the result iterator in a peekableRowIter and call Peek() to read the first row from the result iterator.
	// This is necessary for insert operations, since the insert happens inside the result iterator logic. Without
	// calling this now, insert statements and some DML statements (e.g. CREATE PROCEDURE) would not be executed yet,
	// and future statements in a multi-statement query that depend on those results would fail.
	// Note that we don't worry about the result or the error here – we just want to exercise the iterator code to
	// ensure the statement is executed. If an error does occur, we want that error to be returned in the Next()
	// codepath, not here.
	peekIter := peekableRowIter{iter: rowIter}
	_, _ = peekIter.Peek(stmt.gmsCtx)

	return &doltRows{
		sch:     sch,
		rowIter: &peekIter,
		gmsCtx:  stmt.gmsCtx,
	}, nil
}
