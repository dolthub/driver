package embedded

import (
	"context"
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
var _ driver.StmtExecContext = (*doltMultiStmt)(nil)
var _ driver.StmtQueryContext = (*doltMultiStmt)(nil)

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

func (d doltMultiStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Execute each statement sequentially and return the last result.
	var res driver.Result
	var err error
	for _, stmt := range d.stmts {
		res, err = stmt.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
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

func (d doltMultiStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Query each statement; return the first query result set, matching existing behavior.
	var ret doltMultiRows
	for _, stmt := range d.stmts {
		st := stmt
		ret.rowSets = append(ret.rowSets, func() *doltRows {
			rows, err := st.QueryContext(ctx, args)
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
			if err := rows.Close(); err != nil {
				return nil, err
			}
		}
	}
	return &ret, nil
}

// doltStmt represents a single statement to be executed against a Dolt database.
type doltStmt struct {
	conn  *DoltConn
	query string
}

var _ driver.Stmt = (*doltStmt)(nil)
var _ driver.StmtExecContext = (*doltStmt)(nil)
var _ driver.StmtQueryContext = (*doltStmt)(nil)

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
	named := namedFromValues(args)
	return stmt.ExecContext(context.Background(), named)
}

func (stmt *doltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	vals := make([]driver.Value, len(args))
	for i := range args {
		vals[i] = args[i].Value
	}

	bindings, err := argsToBindings(vals)
	if err != nil {
		return nil, err
	}

	res, err := stmt.conn.runExecWithRetry(ctx, func(se *engine.SqlEngine, gmsCtx *gms.Context) (driver.Result, error) {
		sch, itr, _, err := se.GetUnderlyingEngine().QueryWithBindings(gmsCtx, stmt.query, nil, bindings, nil)
		if err != nil {
			return nil, err
		}
		res := newResult(gmsCtx, sch, itr)
		if res.err != nil {
			return nil, res.err
		}
		return res, nil
	})
	if err != nil {
		return nil, translateIfNeeded(err)
	}
	return res, nil
}

func (stmt *doltStmt) execWithArgs(ctx context.Context, args []driver.Value) (gms.Schema, gms.RowIter, *gms.Context, error) {
	bindings, err := argsToBindings(args)
	if err != nil {
		return nil, nil, nil, err
	}

	return stmt.conn.runQueryWithRetry(ctx, func(se *engine.SqlEngine, gmsCtx *gms.Context) (gms.Schema, gms.RowIter, error) {
		sch, itr, _, err := se.GetUnderlyingEngine().QueryWithBindings(gmsCtx, stmt.query, nil, bindings, nil)
		if err != nil {
			return nil, nil, err
		}

		// Eagerly pull one row to force DML side effects and surface errors early.
		// This is critical for retrying transient errors like "database is read only"
		// which can be raised during iterator consumption.
		peekIter := peekableRowIter{iter: itr}
		_, peekErr := peekIter.Peek(gmsCtx)
		if peekErr != nil {
			// Only treat retryable peek errors as fatal here; preserve existing behavior
			// of returning errors via result iteration for non-retryable cases.
			if isRetryableEmbeddedErr(peekErr) || isRetryableEmbeddedErr(translateError(peekErr)) {
				_ = itr.Close(gmsCtx)
				return nil, nil, peekErr
			}
		}

		return sch, &peekIter, nil
	})
}

// Query executes a query that may return rows, such as a SELECT
func (stmt *doltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.QueryContext(context.Background(), namedFromValues(args))
}

func (stmt *doltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var sch gms.Schema
	var rowIter gms.RowIter
	var gmsCtx *gms.Context
	var err error

	if len(args) != 0 {
		vals := make([]driver.Value, len(args))
		for i := range args {
			vals[i] = args[i].Value
		}
		sch, rowIter, gmsCtx, err = stmt.execWithArgs(ctx, vals)
	} else {
		sch, rowIter, gmsCtx, err = stmt.conn.runQueryWithRetry(ctx, func(se *engine.SqlEngine, gmsCtx *gms.Context) (gms.Schema, gms.RowIter, error) {
			sch, itr, _, err := se.Query(gmsCtx, stmt.query)
			if err != nil {
				return nil, nil, err
			}

			peekIter := peekableRowIter{iter: itr}
			_, peekErr := peekIter.Peek(gmsCtx)
			if peekErr != nil {
				if isRetryableEmbeddedErr(peekErr) || isRetryableEmbeddedErr(translateError(peekErr)) {
					_ = itr.Close(gmsCtx)
					return nil, nil, peekErr
				}
			}

			return sch, &peekIter, nil
		})
	}
	if err != nil {
		return nil, translateIfNeeded(err)
	}

	// Determine whether this is a query result set from the already-peeked iterator.
	peekIter, ok := rowIter.(*peekableRowIter)
	if !ok {
		peekIter = &peekableRowIter{iter: rowIter}
		_, _ = peekIter.Peek(gmsCtx)
	}
	row := gms.Row(nil)
	if len(peekIter.peeks) > 0 {
		row = peekIter.peeks[0]
	}

	return &doltRows{
		sch:              sch,
		rowIter:          peekIter,
		gmsCtx:           gmsCtx,
		isQueryResultSet: isQueryResultSet(row),
	}, nil
}

func namedFromValues(vals []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(vals))
	for i := range vals {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: vals[i]}
	}
	return args
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
