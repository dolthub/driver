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
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestDoltRowsColumnTypeDatabaseTypeName(t *testing.T) {
	sch := gms.Schema{
		&gms.Column{Name: "pk", Type: types.Int64},
		&gms.Column{Name: "name", Type: types.Text},
	}
	rows := &doltRows{sch: sch}

	require.Equal(t, sch[0].Type.String(), rows.ColumnTypeDatabaseTypeName(0))
	require.Equal(t, sch[1].Type.String(), rows.ColumnTypeDatabaseTypeName(1))
	require.Equal(t, "", rows.ColumnTypeDatabaseTypeName(-1))
	require.Equal(t, "", rows.ColumnTypeDatabaseTypeName(2))
}

func TestDoltMultiRowsColumnTypeDatabaseTypeName(t *testing.T) {
	t.Run("no current row set", func(t *testing.T) {
		rows := &doltMultiRows{}
		require.Equal(t, "", rows.ColumnTypeDatabaseTypeName(0))
	})

	t.Run("delegates to current row set", func(t *testing.T) {
		sch := gms.Schema{
			&gms.Column{Name: "v", Type: types.Float64},
		}
		rows := &doltMultiRows{
			currentRowSet: &doltRows{sch: sch},
		}

		require.Equal(t, sch[0].Type.String(), rows.ColumnTypeDatabaseTypeName(0))
		require.Equal(t, "", rows.ColumnTypeDatabaseTypeName(1))
	})
}
