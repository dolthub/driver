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
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	ctx  context.Context
	conn *DoltConn
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	ctx, _, iter, err := tx.conn.queryWithBindings(tx.ctx, "commit", nil)
	if err != nil {
		return err
	}
	if err := iter.Close(ctx); err != nil {
		return translateError(err)
	}
	return nil
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	ctx, _, iter, err := tx.conn.queryWithBindings(tx.ctx, "rollback", nil)
	if err != nil {
		return err
	}
	if err := iter.Close(ctx); err != nil {
		return translateError(err)
	}
	return nil
}
