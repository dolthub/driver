package embedded

import (
	"database/sql/driver"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"strconv"
	"time"
)

var _ driver.Stmt = (*doltStmt)(nil)

type doltStmt struct {
	se     *engine.SqlEngine
	gmsCtx *gms.Context
	query  string
}

// Close closes the statement.
func (stmt *doltStmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (stmt *doltStmt) NumInput() int {
	return -1
}

func argsToBindings(args []driver.Value) (map[string]gms.Expression, error) {
	bindings := make(map[string]gms.Expression)
	for i := range args {
		vIdx := "v" + strconv.FormatInt(int64(i+1), 10)

		var expr gms.Expression
		switch val := args[i].(type) {
		case nil:
			expr = nil
		case string:
			expr = expression.NewLiteral(val, types.LongText)
		case int:
			expr = expression.NewLiteral(int64(val), types.Int64)
		case int8:
			expr = expression.NewLiteral(val, types.Int8)
		case int16:
			expr = expression.NewLiteral(val, types.Int16)
		case int32:
			expr = expression.NewLiteral(val, types.Int32)
		case int64:
			expr = expression.NewLiteral(val, types.Int64)
		case uint:
			expr = expression.NewLiteral(uint64(val), types.Uint64)
		case uint8:
			expr = expression.NewLiteral(val, types.Uint8)
		case uint16:
			expr = expression.NewLiteral(val, types.Uint16)
		case uint32:
			expr = expression.NewLiteral(val, types.Uint32)
		case uint64:
			expr = expression.NewLiteral(val, types.Uint64)
		case float32:
			expr = expression.NewLiteral(val, types.Float32)
		case float64:
			expr = expression.NewLiteral(val, types.Float64)
		case bool:
			expr = expression.NewLiteral(val, types.Boolean)
		case []byte:
			expr = expression.NewLiteral(val, types.Blob)
		case time.Time:
			expr = expression.NewLiteral(val, types.Time)
		default:
			return nil, fmt.Errorf("argument is an unsupported type: '%v'", args[i])
		}

		bindings[vIdx] = expr
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

	sch, itr, err := stmt.se.GetUnderlyingEngine().QueryWithBindings(stmt.gmsCtx, stmt.query, bindings)
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
