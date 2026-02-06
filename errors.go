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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/go-sql-driver/mysql"
)

// translateError converts a go-mysql-server error into a go-sql-driver/mysql
// *MySQLError. This improves compatibility with clients that program against
// embedded and sql-server Dolt.
func translateError(err error) error {
	if err == nil {
		return nil
	}
	vitessErr := sql.CastSQLError(err)
	return &mysql.MySQLError{
		Number:  uint16(vitessErr.Num),
		Message: vitessErr.Message,
	}
}
