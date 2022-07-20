package embedded

import (
	"database/sql/driver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Tx = (*DoltTx)(nil)

type DoltTx struct {
	sqlCtx *gms.Context
	se     *engine.SqlEngine
}

func (tx *DoltTx) Commit() error {
	_, _, err := tx.se.Query(tx.sqlCtx, "COMMIT;")
	return err
}

func (tx *DoltTx) Rollback() error {
	_, _, err := tx.se.Query(tx.sqlCtx, "ROLLBACK;")
	return err
}
