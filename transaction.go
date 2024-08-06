package embedded

import (
	"database/sql/driver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	gmsCtx *gms.Context
	se     *engine.SqlEngine
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	_, _, _, err := tx.se.Query(tx.gmsCtx, "COMMIT;")
	return translateError(err)
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	_, _, _, err := tx.se.Query(tx.gmsCtx, "ROLLBACK;")
	return translateError(err)
}
