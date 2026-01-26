package embedded

import (
	"database/sql/driver"
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	conn *DoltConn
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	se, gmsCtx := tx.conn.getEngineAndContext()
	_, _, _, err := se.Query(gmsCtx, "COMMIT;")
	tx.conn.endTx()
	return translateError(err)
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	se, gmsCtx := tx.conn.getEngineAndContext()
	_, _, _, err := se.Query(gmsCtx, "ROLLBACK;")
	tx.conn.endTx()
	return translateError(err)
}
