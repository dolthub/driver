package embedded

import (
	"database/sql/driver"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	conn   *DoltConn
	gmsCtx *gms.Context
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	se, gmsCtx := tx.conn.getEngineAndContext()
	if tx.gmsCtx != nil {
		gmsCtx = tx.gmsCtx
	}
	_, _, _, err := se.Query(gmsCtx, "COMMIT;")
	if err == nil {
		tx.conn.endTx()
	}
	return translateError(err)
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	se, gmsCtx := tx.conn.getEngineAndContext()
	if tx.gmsCtx != nil {
		gmsCtx = tx.gmsCtx
	}
	_, _, _, err := se.Query(gmsCtx, "ROLLBACK;")
	if err == nil {
		tx.conn.endTx()
	}
	return translateError(err)
}
