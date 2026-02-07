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
