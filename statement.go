package embedded

import (
	"database/sql/driver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
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

func (stmt *DoltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &DoltResult{}, nil
}

func (stmt *DoltStmt) Query(args []driver.Value) (driver.Rows, error) {
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
