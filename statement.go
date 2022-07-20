package embedded

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strconv"
	"time"
)

var _ driver.Stmt = (*DoltStmt)(nil)

type DoltStmt struct {
	se       *engine.SqlEngine
	SqlCtx   *gms.Context
	QueryStr string
}

func (stmt *DoltStmt) Close() error {
	return nil
}

func (stmt *DoltStmt) NumInput() int {
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
			expr = expression.NewLiteral(val, gms.LongText)
		case int:
			expr = expression.NewLiteral(int64(val), gms.Int64)
		case int8:
			expr = expression.NewLiteral(val, gms.Int8)
		case int16:
			expr = expression.NewLiteral(val, gms.Int16)
		case int32:
			expr = expression.NewLiteral(val, gms.Int32)
		case int64:
			expr = expression.NewLiteral(val, gms.Int64)
		case uint:
			expr = expression.NewLiteral(uint64(val), gms.Uint64)
		case uint8:
			expr = expression.NewLiteral(val, gms.Uint8)
		case uint16:
			expr = expression.NewLiteral(val, gms.Uint16)
		case uint32:
			expr = expression.NewLiteral(val, gms.Uint32)
		case uint64:
			expr = expression.NewLiteral(val, gms.Uint64)
		case float32:
			expr = expression.NewLiteral(val, gms.Float32)
		case float64:
			expr = expression.NewLiteral(val, gms.Float64)
		case bool:
			expr = expression.NewLiteral(val, gms.Boolean)
		case []byte:
			expr = expression.NewLiteral(val, gms.Blob)
		case time.Time:
			expr = expression.NewLiteral(val, gms.Time)
		default:
			return nil, fmt.Errorf("argument is an unsupported type: '%v'", args[i])
		}

		bindings[vIdx] = expr
	}

	return bindings, nil
}

func (stmt *DoltStmt) Exec(args []driver.Value) (driver.Result, error) {
	bindings, err := argsToBindings(args)
	if err != nil {
		return nil, err
	}

	sch, itr, err := stmt.se.GetUnderlyingEngine().QueryWithBindings(stmt.SqlCtx, stmt.QueryStr, bindings)
	if err != nil {
		return nil, err
	}

	return NewResult(stmt.SqlCtx, sch, itr), nil
}

func (stmt *DoltStmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, errors.New("not implemented")
	}

	sch, rowIter, err := stmt.se.Query(stmt.SqlCtx, stmt.QueryStr)
	if err != nil {
		return nil, err
	}

	return &DoltRows{
		Schema:  sch,
		RowIter: rowIter,
		SqlCtx:  stmt.SqlCtx,
	}, nil
}
