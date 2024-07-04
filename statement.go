package embedded

import (
	"database/sql/driver"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"strconv"
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

func (d doltMultiStmt) Exec(args []driver.Value) (result driver.Result, err error) {
	// TODO: Do we need a doltMultiResult? Doesn't seem like the driver package
	//       supports multiple results from an exec statement.
	for _, stmt := range d.stmts {
		result, err = stmt.Exec(args)
		if err != nil {
			// If any error occurs, return the error and stop executing statements
			return nil, err
		}
	}

	// return the last result
	return result, nil
}

func (d doltMultiStmt) Query(args []driver.Value) (driver.Rows, error) {
	var multiResultSet doltMultiRows
	for _, stmt := range d.stmts {
		rows, err := stmt.Query(args)
		if err != nil {
			// If any error occurs, return the error and stop executing statements
			return nil, err
		}
		multiResultSet.rowSets = append(multiResultSet.rowSets, rows.(*doltRows))
	}
	return &multiResultSet, nil
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

	return &doltRows{
		sch:     sch,
		rowIter: rowIter,
		gmsCtx:  stmt.gmsCtx,
	}, nil
}
