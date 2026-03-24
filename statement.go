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
	"database/sql/driver"
	"errors"
	"strconv"

	"github.com/dolthub/vitess/go/vt/sqlparser"

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

func (d doltMultiStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec called on doltMultiStmt; use ExecContext")
}

func (d doltMultiStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (result driver.Result, err error) {
	for _, stmt := range d.stmts {
		result, err = stmt.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (d doltMultiStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var ret doltMultiRows
	for _, stmt := range d.stmts {
		ret.rowSets = append(ret.rowSets, func() (*doltRows, error) {
			rows, err := stmt.QueryContext(ctx, args)
			if err != nil {
				return nil, err
			}
			return rows.(*doltRows), nil
		})
	}
	for ret.currentIdx = 0; ret.currentIdx < len(ret.rowSets); ret.currentIdx++ {
		rows, err := ret.rowSets[ret.currentIdx]()
		if err != nil {
			return nil, err
		} else if rows.isQueryResultSet() {
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

func (d doltMultiStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query called on doltMultiStmt; use QueryContext")
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

func namedArgsToBindings(args []driver.NamedValue) (map[string]sqlparser.Expr, error) {
	bindings := make(map[string]sqlparser.Expr)
	for _, arg := range args {
		var key string
		if arg.Name != "" {
			key = arg.Name
		} else {
			key = "v" + strconv.FormatInt(int64(arg.Ordinal), 10)
		}
		bv, err := sqltypes.BuildBindVariable(arg.Value)
		if err != nil {
			return nil, err
		}
		v, err := sqltypes.BindVariableToValue(bv)
		if err != nil {
			return nil, err
		}
		bindings[key], err = sqlparser.ExprFromValue(v)
		if err != nil {
			return nil, err
		}
	}
	return bindings, nil
}

// execCore runs the query with the given bindings against the provided gmsCtx.
func (stmt *doltStmt) execCore(gmsCtx *gms.Context, bindings map[string]sqlparser.Expr) (gms.Schema, gms.RowIter, error) {
	sch, itr, _, err := stmt.conn.se.GetUnderlyingEngine().QueryWithBindings(gmsCtx, stmt.query, nil, bindings, nil)
	return sch, itr, err
}

func (stmt *doltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec called on doltStmt; use ExecContext")
}

// ExecContext implements driver.StmtExecContext.
func (stmt *doltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	queryCtx, err := stmt.conn.beginQuery(ctx, stmt.query)
	if err != nil {
		return nil, err
	}
	defer stmt.conn.endQuery(queryCtx)
	bindings, err := namedArgsToBindings(args)
	if err != nil {
		return nil, err
	}
	sch, itr, err := stmt.execCore(queryCtx, bindings)
	if err != nil {
		return nil, translateError(err)
	}
	res, err := newResult(queryCtx, sch, itr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (stmt *doltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query called on doltStmt; use QueryContext")
}

// QueryContext implements driver.StmtQueryContext.
func (stmt *doltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryCtx, err := stmt.conn.beginQuery(ctx, stmt.query)
	if err != nil {
		return nil, err
	}

	var sch gms.Schema
	var rowIter gms.RowIter

	if len(args) != 0 {
		bindings, bindErr := namedArgsToBindings(args)
		if bindErr != nil {
			stmt.conn.endQuery(queryCtx)
			return nil, bindErr
		}
		sch, rowIter, err = stmt.execCore(queryCtx, bindings)
	} else {
		sch, rowIter, _, err = stmt.conn.se.Query(queryCtx, stmt.query)
	}
	if err != nil {
		stmt.conn.endQuery(queryCtx)
		return nil, translateError(err)
	}

	return &doltRows{
		sch:     sch,
		rowIter: rowIter,
		gmsCtx:  queryCtx,
		conn:    stmt.conn,
	}, nil
}

// isQueryResultSet returns true if the schema indicates the statement produces a SELECT-style
// result set. Non-query statements (DML, DDL) return types.OkResultSchema; some statements
// return a nil schema with an empty iterator. Both must be drained eagerly and not exposed
// to the caller as result sets.
func isQueryResultSet(sch gms.Schema) bool {
	return sch != nil && !types.IsOkResultSchema(sch)
}
