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
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestTranslateError(t *testing.T) {
	tests := []struct {
		original       error
		expectedNumber uint16
	}{
		{
			sql.ErrLockDeadlock.New("could not serialize transaction"),
			1213,
		},
		{
			nil,
			0,
		},
	}
	for _, test := range tests {
		err := translateError(test.original)
		if err == nil {
			require.Equal(t, uint16(0), test.expectedNumber)
		} else {
			var mysqlErr *mysql.MySQLError
			require.True(t, errors.As(err, &mysqlErr))
			require.Equal(t, test.expectedNumber, mysqlErr.Number)
		}
	}
}
