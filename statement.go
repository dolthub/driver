package embedded

import (
	"database/sql/driver"
	"strconv"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
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
	for _, stmt := range d.stmts {
		result, err = stmt.Exec(args)
		if err != nil {
			// If any error occurs, return the error and don't execute any more statements
			return nil, err
		}
	}

	// Otherwise, return the last result, to match the MySQL driver's behavior
	return result, nil
}

func (d doltMultiStmt) Query(args []driver.Value) (driver.Rows, error) {
	var ret doltMultiRows
	for _, stmt := range d.stmts {
		ret.rowSets = append(ret.rowSets, func()*doltRows{
			rows, err := stmt.Query(args)
			if err != nil {
				return &doltRows{err: err}
			}
			return rows.(*doltRows)
		})
	}
	for ret.currentIdx = 0; ret.currentIdx < len(ret.rowSets); ret.currentIdx++ {
		rows := ret.rowSets[ret.currentIdx]()
		if rows.err != nil {
			return nil, rows.err
		} else if rows.isQueryResultSet {
			ret.currentRowSet = rows
			break
		} else {
			err := rows.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	return &ret, nil
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

func argsToBindings(args []driver.Value) (map[string]sqlparser.Expr, error) {
	bindings := make(map[string]sqlparser.Expr)
	for i := range args {
		vIdx := "v" + strconv.FormatInt(int64(i+1), 10)
		bv, err := sqltypes.BuildBindVariable(args[i])
		if err != nil {
			return nil, err
		}
		v, err := sqltypes.BindVariableToValue(bv)
		if err != nil {
			return nil, err
		}
		bindings[vIdx], err = sqlparser.ExprFromValue(v)
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

	sch, itr, _, err := stmt.se.GetUnderlyingEngine().QueryWithBindings(stmt.gmsCtx, stmt.query, nil, bindings, nil)
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
		sch, rowIter, _, err = stmt.se.Query(stmt.gmsCtx, stmt.query)
	}
	if err != nil {
		return nil, translateError(err)
	}

	// Wrap the result iterator in a peekableRowIter and call Peek() to read the first row from the result iterator.
	// This is necessary for insert operations, since the insert happens inside the result iterator logic. Without
	// calling this now, insert statements and some DML statements (e.g. CREATE PROCEDURE) would not be executed yet,
	// and future statements in a multi-statement query that depend on those results would fail.
	// If an error does occur, we want that error to be returned in the Next() codepath, not here.
	peekIter := peekableRowIter{iter: rowIter}
	row, _ := peekIter.Peek(stmt.gmsCtx)

	return &doltRows{
		sch:              sch,
		rowIter:          &peekIter,
		gmsCtx:           stmt.gmsCtx,
		isQueryResultSet: isQueryResultSet(row),
	}, nil
}

// isQueryResultSet returns true if the specified |row| is a valid result set for a query. If row only contains
// one column and is an OkResult, or if row has zero columns, then the statement that generated this row was not
// a query.
func isQueryResultSet(row gms.Row) bool {
	// If row is nil, return true since this could still be a valid, empty result set.
	if row == nil {
		return true
	}

	if len(row) == 1 {
		if _, ok := row[0].(types.OkResult); ok {
			return false
		}
	} else if len(row) == 0 {
		return false
	}

	return true
}
