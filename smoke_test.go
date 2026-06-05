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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runTestsAgainstMySQL can be set to true to run tests against a MySQL database using the MySQL driver.
// This is useful to test behavior compatibility between the Dolt driver and the MySQL driver. We
// want the Dolt driver to have the same semantics/behavior as the MySQL driver, so that customers
// familiar with using the MySQL driver, or code already using the MySQL driver, can easily switch
// to the Dolt driver. When this option is enabled, the MySQL database connection can be configured
// using mysqlDsn below.
var runTestsAgainstMySQL = false

// mysqlDsn specifies the connection string for a MySQL database. Used only when the
// runTestsAgainstMySQL variable above is enabled.
var mysqlDsn = "root@tcp(localhost:3306)/?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true"

// TestPreparedStatements tests that values can be plugged into "?" placeholders in queries.
func TestPreparedStatements(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "create table prepTest (id int, name varchar(256));")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	rows, err = conn.QueryContext(ctx, "insert into prepTest VALUES (?, ?);", 10, "foo")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

func TestMultiStatements(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "show tables like 'schema_migrations'")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	res, err := conn.ExecContext(ctx, "create table testtable (id int primary key, name varchar(256))")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	rows, err = conn.QueryContext(ctx, "insert into testtable values (1, 'aaron'),(2, 'brian'); insert into testtable values (3, 'tim'); select * from testtable")
	require.NoError(t, err)
	var id int
	var name string

	// NOTE: Because the first two statements are not queries and don't have real result sets, the current result set
	//       is automatically positioned at the third statement.
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 1, id)
	require.Equal(t, "aaron", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 2, id)
	require.Equal(t, "brian", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 3, id)
	require.Equal(t, "tim", name)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	rows, err = conn.QueryContext(ctx, "select * from testtable; select * from doesnotexist; select * from testtable")
	require.NoError(t, err)

	// The first result set contains all the rows from testtable
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 1, id)
	require.Equal(t, "aaron", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 2, id)
	require.Equal(t, "brian", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 3, id)
	require.Equal(t, "tim", name)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	// The second result set has an error
	require.False(t, rows.NextResultSet())
	require.NotNil(t, rows.Err())
	// MySQL returns a slightly different error message than Dolt
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1146: table not found: doesnotexist", rows.Err().Error())
	} else {
		require.Equal(t, "Error 1146 (42S02): Table 'testdb.doesnotexist' doesn't exist", rows.Err().Error())
	}

	// The third result set should have more rows... but we can't access them after the
	// error in the second result set. This is the same behavior as the MySQL driver
	require.False(t, rows.NextResultSet())
	require.NotNil(t, rows.Err())
}

// TestMultiStatementsExecContext tests that using ExecContext to run a multi-statement query works as expected and
// matches the behavior of the MySQL driver.
func TestMultiStatementsExecContext(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	_, err := conn.ExecContext(ctx, "CREATE TABLE example_table (id int, name varchar(256));")
	require.NoError(t, err)

	// ExecContext returns the results from the LAST statement executed. This differs from the behavior for QueryContext.
	result, err := conn.ExecContext(ctx, "INSERT into example_table VALUES (999, 'boo'); "+
		"INSERT into example_table VALUES (998, 'foo'); INSERT into example_table VALUES (997, 'goo'), (996, 'loo');")
	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 2, rowsAffected)

	// Assert that all statements were correctly executed
	requireResults(t, conn, "SELECT * FROM example_table ORDER BY id;",
		[][]any{{996, "loo"}, {997, "goo"}, {998, "foo"}, {999, "boo"}})

	// ExecContext returns an error if ANY of the statements can't be executed. This also differs from the behavior of QueryContext.
	_, err = conn.ExecContext(ctx, "INSERT into example_table VALUES (100, 'woo'); "+
		"INSERT into example_table VALUES (1, 2, 'too many'); SET @allStatementsExecuted=1;")
	require.NotNil(t, err)
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1105: number of values does not match number of columns provided", err.Error())
	} else {
		require.Equal(t, "Error 1136 (21S01): Column count doesn't match value count at row 1", err.Error())
	}

	// Assert that the first insert statement was executed before the error occurred
	requireResults(t, conn, "SELECT * FROM example_table ORDER BY id;",
		[][]any{{100, "woo"}, {996, "loo"}, {997, "goo"}, {998, "foo"}, {999, "boo"}})

	// Once an error occurs, additional statements are NOT executed. This code tests that the last SET statement
	// above was NOT executed.
	requireResults(t, conn, "SELECT @allStatementsExecuted;", [][]any{{nil}})
}

// TestMultiStatementsQueryContext tests that using QueryContext to run a multi-statement query works as expected and
// matches the behavior of the MySQL driver.
func TestMultiStatementsQueryContext(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	// QueryContext returns the results from the FIRST statement executed. This differs from the behavior for ExecContext.
	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "SELECT 1 FROM dual; SELECT 2 FROM dual; ")
	require.NoError(t, err)
	require.NoError(t, rows.Err())

	var v any
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 1, v)
	require.False(t, rows.Next())

	require.True(t, rows.NextResultSet())
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 2, v)
	require.False(t, rows.Next())

	require.False(t, rows.NextResultSet())
	require.NoError(t, rows.Close())

	// QueryContext returns an error only if the FIRST statement can't be executed.
	rows, err = conn.QueryContext(ctx, "SELECT * FROM no_table; SELECT 42 FROM dual;")
	require.Nil(t, rows)
	require.NotNil(t, err)
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1146: table not found: no_table", err.Error())
	} else {
		require.Equal(t, "Error 1146 (42S02): Table 'testdb.no_table' doesn't exist", err.Error())
	}

	// To access the error for statements after the first statement, you must use rows.Err()
	rows, err = conn.QueryContext(ctx, "SELECT 42 FROM dual; SELECT * FROM no_table; SET @allStatementsExecuted=1;")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 42, v)
	require.False(t, rows.Next())
	require.False(t, rows.NextResultSet())
	require.NotNil(t, rows.Err())
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1146: table not found: no_table", rows.Err().Error())
	} else {
		require.Equal(t, "Error 1146 (42S02): Table 'testdb.no_table' doesn't exist", rows.Err().Error())
	}
	require.NoError(t, rows.Close())

	// Once an error occurs, additional statements are NOT executed. This code tests that the last SET statement
	// above was NOT executed.
	rows, err = conn.QueryContext(ctx, "SELECT @allStatementsExecuted;")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Nil(t, v)
	require.NoError(t, rows.Close())

	// Non-query statements don't return a real result set, so they are skipped over automatically
	rows, err = conn.QueryContext(ctx, "SET @notUsed=1; SELECT 42 FROM dual; ")
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 42, v)
	require.NoError(t, rows.Close())

	// Queries that generate an empty result set are NOT skipped over automatically
	rows, err = conn.QueryContext(ctx, "CREATE TABLE t (pk int primary key); SELECT * FROM t; SELECT 42 FROM dual;")
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())
	require.True(t, rows.NextResultSet())
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 42, v)
	require.NoError(t, rows.Close())

	// If an error occurs between two valid queries, NextResulSet() returns false and exposes the
	// error from rows.Err().
	rows, err = conn.QueryContext(ctx, "SELECT * FROM t; SELECT * from t2; SELECT 42 FROM dual;")
	require.NoError(t, err)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())
	require.False(t, rows.NextResultSet())
	require.NotNil(t, rows.Err())
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1146: table not found: t2", rows.Err().Error())
	} else {
		require.Equal(t, "Error 1146 (42S02): Table 'testdb.t2' doesn't exist", rows.Err().Error())
	}
	require.NoError(t, rows.Close())

	// If an error occurs before the first real query results set, the error is returned, with no rows
	rows, err = conn.QueryContext(ctx, "set @foo='bar'; SELECT * from t2; SELECT 42 FROM dual;")
	require.NotNil(t, err)
	require.Nil(t, rows)
	if !runTestsAgainstMySQL {
		require.Equal(t, "Error 1146: table not found: t2", err.Error())
	} else {
		require.Equal(t, "Error 1146 (42S02): Table 'testdb.t2' doesn't exist", err.Error())
	}
}

// TestMultiStatementsWithNoSpaces tests that multistatements are parsed correctly, even when
// there is no space between the statement delimiter and the next statement.
func TestMultiStatementsWithNoSpaces(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	var v int
	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "select 42 from dual;select 43 from dual;")

	// Check the first result set
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Equal(t, 42, v)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())

	// Check the second result set
	require.True(t, rows.NextResultSet())
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Equal(t, 43, v)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestMultiStatementsWithEmptyStatements tests that any empty statements in a multistatement query are skipped over.
// This includes statements that are entirely empty, as well as statements that contain only comments.
func TestMultiStatementsWithEmptyStatements(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	var v int
	ctx := t.Context()

	// Test that empty statements don't return errors and don't return result sets
	rows, err := conn.QueryContext(ctx, "select 42 from dual; # This is an empty statement")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Equal(t, 42, v)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())
	require.False(t, rows.NextResultSet())
	require.NoError(t, rows.Close())

	// Test another form of empty statement
	rows, err = conn.QueryContext(ctx, "select 42 from dual; ; ; ; select 24 from dual; ;")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.Equal(t, 42, v)
	require.NoError(t, rows.Err())
	require.False(t, rows.Next())

	// NOTE: The MySQL driver does not allow moving past empty statements to the next result set
	if !runTestsAgainstMySQL {
		require.True(t, rows.NextResultSet())
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&v))
		require.Equal(t, 24, v)
		require.NoError(t, rows.Err())
		require.False(t, rows.Next())
	}

	require.False(t, rows.NextResultSet())
	require.NoError(t, rows.Close())
}

func TestMultiStatementsStoredProc(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "create procedure p() begin select 1; end; call p(); call p(); call p();")
	require.NoError(t, err)

	// NOTE: Because the first statement is not a query and doesn't have a real result set, the current result set
	//       is automatically positioned at the second statement.
	for rows.Next() {
		var i int
		err = rows.Scan(&i)
		require.NoError(t, err)
		require.Equal(t, 1, i)
	}
	require.NoError(t, rows.Err())

	// Advance to the third result set and check its rows
	require.True(t, rows.NextResultSet())
	for rows.Next() {
		var i int
		err = rows.Scan(&i)
		require.NoError(t, err)
		require.Equal(t, 1, i)
	}
	require.NoError(t, rows.Err())

	// Advance to the fourth result set and check its rows
	require.True(t, rows.NextResultSet())
	for rows.Next() {
		var i int
		err = rows.Scan(&i)
		require.NoError(t, err)
		require.Equal(t, 1, i)
	}
	require.NoError(t, rows.Err())

	require.NoError(t, rows.Close())
}

func TestMultiStatementsTrigger(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	res, err := conn.ExecContext(ctx, "create table t (i int primary key, j int);")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "create trigger trig before insert on t for each row begin set new.j = new.j * 100; end; insert into t values (1, 2); select * from t;")
	require.NoError(t, err)

	// NOTE: Because the first statement is not a query and doesn't have a real result set, the current result set
	//       is automatically positioned at the second statement.
	for rows.Next() {
		var i, j int
		err = rows.Scan(&i, &j)
		require.NoError(t, err)
		require.Equal(t, 1, i)
		require.Equal(t, 200, j)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

// TestClientFoundRows asserts that the number of affected rows reported for a query
// correctly reflects whether the CLIENT_FOUND_ROWS capability is set or not.
func TestClientFoundRows(t *testing.T) {
	queries := []string{
		"insert into testtable values (1,'aaron'),(2,'brian'),(3,'tim')",
		"insert into testtable values (4,'aaron2'),(2,'brian2'),(3,'tim') on duplicate key update name = VALUES(name)",
		"update testtable set name = (case id when 1 then 'aaron' when 2 then 'tim2' when 4 then 'aaron2' end) where id in (1,3,4)",
	}

	tests := []struct {
		name            string
		clientFoundRows bool
		expectedRows    []int64
	}{
		{
			name:            "client_found_rows_disabled",
			clientFoundRows: false,
			expectedRows:    []int64{3, 3, 1},
		},
		{
			name:            "client_found_rows_enabled",
			clientFoundRows: true,
			expectedRows:    []int64{3, 4, 3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, conn := initializeTestDatabaseConnection(t, test.clientFoundRows)
			t.Cleanup(func() {
				require.NoError(t, conn.Close())
			})
			ctx := t.Context()

			res, err := conn.ExecContext(ctx, "create table testtable (id int primary key, name varchar(256))")
			require.NoError(t, err)
			_, err = res.RowsAffected()
			require.NoError(t, err)

			for i, query := range queries {
				res, err := conn.ExecContext(ctx, query)
				require.NoError(t, err)
				rowsAffected, err := res.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, test.expectedRows[i], rowsAffected)
			}
		})
	}
}

// TestQueryContextInitialization asserts that the context is correctly initialized for each query, including
// setting the current time at query execution start.
func TestQueryContextInitialization(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	rows, err := conn.QueryContext(ctx, "select NOW()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var s1, s2 string
	err = rows.Scan(&s1)
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// Pause for 1s, then select NOW() and assert that the two times are different
	time.Sleep(1 * time.Second)
	rows, err = conn.QueryContext(ctx, "SELECT NOW()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	err = rows.Scan(&s2)
	require.NoError(t, err)
	assert.NotEqual(t, s1, s2)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

// TestTypes asserts that various MySQL types are returned as the expected Go type by the driver.
func TestTypes(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()
	_, err := conn.ExecContext(ctx, `
create table testtable (
	enum_col ENUM('a', 'b', 'c'),
	set_col SET('a', 'b', 'c'),
	json_col JSON,
	blob_col BLOB,
	text_col TEXT,
	geom_col POINT,
	date_col DATETIME
);

insert into testtable values ('b', 'a,c', '{"key": 42}', 'data', 'text', Point(5, -5), NOW());
`)
	require.NoError(t, err)

	row := conn.QueryRowContext(ctx, "select * from testtable")
	vals := make([]any, 7)
	ptrs := make([]any, 7)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	require.NoError(t, row.Scan(ptrs...))
	require.EqualValues(t, "b", vals[0])
	require.EqualValues(t, "a,c", vals[1])
	require.EqualValues(t, `{"key": 42}`, vals[2])
	require.EqualValues(t, []byte(`data`), vals[3])
	require.EqualValues(t, "text", vals[4])
	require.IsType(t, []byte(nil), vals[5])
	require.IsType(t, time.Time{}, vals[6])
}

// adaptiveHelperEnvKey guards the helper-process test below so it only runs when this test
// binary is re-executed as a child process by TestAdaptiveEncoding.
const adaptiveHelperEnvKey = "DOLT_DRIVER_ADAPTIVE_HELPER"

// adaptiveHelperDSNEnvKey carries the DSN of the database the helper process should re-verify.
const adaptiveHelperDSNEnvKey = "DOLT_DRIVER_ADAPTIVE_DSN"

// adaptiveRow describes one row inserted by TestAdaptiveEncoding. The values are entirely
// determined by |id| and these flags, so the helper process can recompute the expected
// values without any data being passed across the process boundary.
type adaptiveRow struct {
	id        int
	textLarge bool
	blobLarge bool
	jsonLarge bool
	isNull    bool
}

// adaptiveRows is the fixed set of rows exercised by TestAdaptiveEncoding: all eight
// small/large combinations across the three adaptive types, plus an all-NULL row.
func adaptiveRows() []adaptiveRow {
	return []adaptiveRow{
		{id: 1, textLarge: false, blobLarge: false, jsonLarge: false},
		{id: 2, textLarge: true, blobLarge: false, jsonLarge: false},
		{id: 3, textLarge: false, blobLarge: true, jsonLarge: false},
		{id: 4, textLarge: false, blobLarge: false, jsonLarge: true},
		{id: 5, textLarge: true, blobLarge: true, jsonLarge: false},
		{id: 6, textLarge: true, blobLarge: false, jsonLarge: true},
		{id: 7, textLarge: false, blobLarge: true, jsonLarge: true},
		{id: 8, textLarge: true, blobLarge: true, jsonLarge: true},
		{id: 9, isNull: true},
	}
}

// adaptiveLargeText, adaptiveLargeBlob, and adaptiveLargeJSON each produce a value comfortably
// larger than the 2048 byte tuple length target, so any row containing one is guaranteed to
// spill that field out-of-band. The content is keyed off |id| so we can assert exact
// round-tripping rather than just matching lengths.
func adaptiveLargeText(id int) string {
	seed := fmt.Sprintf("row-%d-text|", id)
	var sb strings.Builder
	for sb.Len() < 16384 {
		sb.WriteString(seed)
	}
	return sb.String()[:16384]
}

func adaptiveLargeBlob(id int) []byte {
	out := make([]byte, 16384)
	for i := range out {
		out[i] = byte(id + i)
	}
	return out
}

func adaptiveLargeJSON(id int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"id": %d, "size": "large", "data": [`, id)
	for i := 0; i < 4096; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%d", i)
	}
	sb.WriteString("]}")
	return sb.String()
}

func adaptiveSmallText(id int) string { return fmt.Sprintf("small-text-%d", id) }
func adaptiveSmallBlob(id int) []byte { return []byte(fmt.Sprintf("small-blob-%d", id)) }
func adaptiveSmallJSON(id int) string { return fmt.Sprintf(`{"id": %d, "size": "small"}`, id) }

// adaptiveExpected holds the values we expect to read back for a row.
type adaptiveExpected struct {
	txt any // string or nil
	blb any // []byte or nil
	jsn any // JSON string or nil
}

// adaptiveWant computes the expected read-back values for every row. Because the values are
// deterministic in |id|, both the parent test and the helper process derive the same map.
func adaptiveWant() map[int]adaptiveExpected {
	rows := adaptiveRows()
	want := make(map[int]adaptiveExpected, len(rows))
	for _, r := range rows {
		if r.isNull {
			want[r.id] = adaptiveExpected{txt: nil, blb: nil, jsn: nil}
			continue
		}
		txt := adaptiveSmallText(r.id)
		if r.textLarge {
			txt = adaptiveLargeText(r.id)
		}
		blb := adaptiveSmallBlob(r.id)
		if r.blobLarge {
			blb = adaptiveLargeBlob(r.id)
		}
		jsn := adaptiveSmallJSON(r.id)
		if r.jsonLarge {
			jsn = adaptiveLargeJSON(r.id)
		}
		want[r.id] = adaptiveExpected{txt: txt, blb: blb, jsn: jsn}
	}
	return want
}

// jsonEqual reports whether two JSON documents are semantically equal, ignoring formatting
// differences such as whitespace introduced when the server re-serializes a value.
func jsonEqual(a, b string) (bool, error) {
	var av, bv any
	if err := json.Unmarshal([]byte(a), &av); err != nil {
		return false, fmt.Errorf("unmarshal expected JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(b), &bv); err != nil {
		return false, fmt.Errorf("unmarshal actual JSON: %w", err)
	}
	return reflect.DeepEqual(av, bv), nil
}

// verifyAdaptive reads every row back over |conn| and checks exact round-tripping against
// |want|. Large fields take the out-of-band ValueContext read path; small fields are inline.
// Both must produce identical values. It returns an error describing the first mismatch so it
// can be used both with require (in the parent test) and with an exit code (in the helper
// process), rather than depending on a *testing.T.
func verifyAdaptive(ctx context.Context, conn *sql.Conn, want map[int]adaptiveExpected) error {
	sqlRows, err := conn.QueryContext(ctx, "select id, txt, blb, jsn from adaptive order by id")
	if err != nil {
		return fmt.Errorf("query adaptive: %w", err)
	}

	seen := 0
	err = func() (err error) {
		defer func() {
			if cerr := sqlRows.Close(); cerr != nil {
				if err == nil {
					err = cerr
				}
			}
		}()
		for sqlRows.Next() {
			var id int
			var txt sql.NullString
			var blb []byte
			var jsn sql.NullString
			if err := sqlRows.Scan(&id, &txt, &blb, &jsn); err != nil {
				return fmt.Errorf("scan: %w", err)
			}

			exp, ok := want[id]
			if !ok {
				return fmt.Errorf("unexpected row id %d", id)
			}

			if exp.txt == nil {
				if txt.Valid {
					return fmt.Errorf("row %d: expected NULL txt, got non-NULL", id)
				}
			} else {
				if !txt.Valid {
					return fmt.Errorf("row %d: expected non-NULL txt", id)
				}
				if txt.String != exp.txt.(string) {
					return fmt.Errorf("row %d: txt mismatch (got len=%d, want len=%d)",
						id, len(txt.String), len(exp.txt.(string)))
				}
			}

			if exp.blb == nil {
				if blb != nil {
					return fmt.Errorf("row %d: expected NULL blb, got non-NULL", id)
				}
			} else {
				if !bytes.Equal(blb, exp.blb.([]byte)) {
					return fmt.Errorf("row %d: blb mismatch (got len=%d, want len=%d)",
						id, len(blb), len(exp.blb.([]byte)))
				}
			}

			if exp.jsn == nil {
				if jsn.Valid {
					return fmt.Errorf("row %d: expected NULL jsn, got non-NULL", id)
				}
			} else {
				if !jsn.Valid {
					return fmt.Errorf("row %d: expected non-NULL jsn", id)
				}
				eq, err := jsonEqual(exp.jsn.(string), jsn.String)
				if err != nil {
					return fmt.Errorf("row %d: %w", id, err)
				}
				if !eq {
					return fmt.Errorf("row %d: jsn mismatch: got %s want %s", id, jsn.String, exp.jsn.(string))
				}
			}
			seen++
		}
		if err := sqlRows.Err(); err != nil {
			return fmt.Errorf("rows err: %w", err)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	if want := len(adaptiveRows()); seen != want {
		return fmt.Errorf("expected to read back %d rows, saw %d", want, seen)
	}

	// Also exercise a filtered read that returns only a subset, mixing inline and out-of-band
	// values, to ensure the read path works outside of a full table scan.
	subRows, err := conn.QueryContext(ctx, "select id from adaptive where id in (2, 8) order by id")
	if err != nil {
		return fmt.Errorf("subset query: %w", err)
	}
	defer subRows.Close()
	var gotIDs []int
	for subRows.Next() {
		var id int
		if err := subRows.Scan(&id); err != nil {
			return fmt.Errorf("subset scan: %w", err)
		}
		gotIDs = append(gotIDs, id)
	}
	if err := subRows.Err(); err != nil {
		return fmt.Errorf("subset rows err: %w", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 2 || gotIDs[1] != 8 {
		return fmt.Errorf("subset mismatch: got %v want [2 8]", gotIDs)
	}
	return nil
}

// TestAdaptiveEncoding exercises the adaptive encoding read path for TEXT, BLOB, and JSON
// columns. Dolt stores these "adaptive" types either inline in the row tuple or out-of-band
// as an address, deciding per-row based on the total tuple size: once a row's inline size
// exceeds the tuple length target (2048 bytes), the largest adaptive fields are spilled
// out-of-band. Out-of-band values are returned by the row iterator as values that must be
// dereferenced against the value store via ValueContext, which is the path rows.go handles.
//
// The test creates a table with one column of each adaptive type and inserts rows covering
// every combination of small (inline) and large (out-of-band) values, plus an all-NULL row,
// then queries them back and asserts the values round-trip exactly.
//
// Every database operation runs in a separate child process, never in this test process:
//
//   - The first child writes the table and rows, then verifies them in that same process
//     (a "warm" read served partly from the process-wide adaptive value cache populated by
//     the writes).
//   - The second child re-verifies against a freshly opened database. Because it is a new
//     process, its adaptive value cache is *cold*, so the out-of-band values must be reloaded
//     from the on-disk value store. That is the read path we actually want to exercise: a
//     warm in-process cache would serve the correct values and mask an incorrect on-disk read.
//
// Running each step in its own process also means this test process never holds the embedded
// journal lock, so the second child can open the same directory. This mirrors the os/exec
// machinery used by the cross-process flock tests.
func TestAdaptiveEncoding(t *testing.T) {
	if runTestsAgainstMySQL {
		t.SkipNow()
	}

	dir := t.TempDir()

	// Write and warm-verify the initial database in one process, then cold-verify in another.
	runAdaptiveHelperProcess(t, "TestHelperProcess_WriteAdaptive", dir)
	runAdaptiveHelperProcess(t, "TestHelperProcess_VerifyAdaptive", dir)
}

// runAdaptiveHelperProcess re-executes this test binary, running only the named helper test
// against the database at |dir|. It fails the test if the child exits non-zero, surfacing the
// child's combined output for diagnosis.
func runAdaptiveHelperProcess(t *testing.T, name, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$") // #nosec G204 -- test binary
	cmd.Env = append(os.Environ(),
		adaptiveHelperEnvKey+"=1",
		adaptiveHelperDSNEnvKey+"="+adaptiveDSN(dir),
	)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s failed:\n%s", name, strings.TrimSpace(string(out)))
}

// adaptiveDSN builds the DSN used to open the embedded database created by TestAdaptiveEncoding,
// matching the query parameters used by openTestDatabaseConnectionAt.
func adaptiveDSN(dir string) string {
	query := url.Values{
		"commitname":      []string{"Billy Batson"},
		"commitemail":     []string{"shazam@gmail.com"},
		"database":        []string{"testdb"},
		"multistatements": []string{"true"},
	}
	dsn := url.URL{Scheme: "file", Path: encodeDir(dir), RawQuery: query.Encode()}
	return dsn.String()
}

// openAdaptiveHelperDB opens the embedded database named by the helper DSN environment variable
// and returns a ready connection. It is used only by the helper-process tests below; on any
// failure it prints a diagnostic and exits the process non-zero so the parent can observe it.
func openAdaptiveHelperDB(ctx context.Context) (*sql.DB, *sql.Conn) {
	dsn := os.Getenv(adaptiveHelperDSNEnvKey)
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "missing "+adaptiveHelperDSNEnvKey)
		os.Exit(2)
	}
	db, err := sql.Open(DoltDriverName, dsn)
	if err != nil {
		fmt.Printf("ERR: sql.Open: %v\n", err)
		os.Exit(1)
	}
	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("ERR: Ping: %v\n", err)
		os.Exit(1)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("ERR: Conn: %v\n", err)
		os.Exit(1)
	}
	return db, conn
}

// TestHelperProcess_WriteAdaptive is executed in a separate process by TestAdaptiveEncoding. It
// creates the testdb database and adaptive table, inserts every row, then verifies the values in
// this same process (a warm read). It only runs when the guard environment variable is set;
// otherwise it is skipped during normal test runs. On success it prints "OK" and exits 0; on any
// failure it prints a diagnostic and exits non-zero.
func TestHelperProcess_WriteAdaptive(t *testing.T) {
	if os.Getenv(adaptiveHelperEnvKey) != "1" {
		t.Skip("helper process only")
	}

	ctx := context.Background()
	db, conn := openAdaptiveHelperDB(ctx)
	defer db.Close()
	defer conn.Close()

	setup := []string{
		"drop database if exists testdb",
		"create database testdb",
		"use testdb",
		"create table adaptive (id int primary key, txt TEXT, blb BLOB, jsn JSON)",
	}
	for _, stmt := range setup {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			fmt.Printf("ERR: %q: %v\n", stmt, err)
			t.FailNow()
		}
	}

	want := adaptiveWant()
	for _, r := range adaptiveRows() {
		exp := want[r.id]
		if _, err := conn.ExecContext(ctx,
			"insert into adaptive (id, txt, blb, jsn) values (?, ?, ?, ?)",
			r.id, exp.txt, exp.blb, exp.jsn); err != nil {
			fmt.Printf("ERR: insert id=%d: %v\n", r.id, err)
			t.FailNow()
		}
	}

	// Verify the freshly-written database in this same process, while the adaptive value cache
	// is warm from the writes above.
	if err := verifyAdaptive(ctx, conn, want); err != nil {
		fmt.Printf("ERR: warm verify: %v\n", err)
		t.FailNow()
	}

	fmt.Println("OK")
}

// TestHelperProcess_VerifyAdaptive is executed in a separate process by TestAdaptiveEncoding to
// re-verify the adaptive-encoding rows against a cold process-wide cache. It only runs when the
// guard environment variable is set; otherwise it is skipped during normal test runs. On success
// it prints "OK" and exits 0; on any failure it prints a diagnostic and exits non-zero so the
// parent can surface it via the child's exit status and combined output.
func TestHelperProcess_VerifyAdaptive(t *testing.T) {
	if os.Getenv(adaptiveHelperEnvKey) != "1" {
		t.Skip("helper process only")
	}

	ctx := context.Background()
	db, conn := openAdaptiveHelperDB(ctx)
	defer db.Close()
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "use testdb"); err != nil {
		fmt.Printf("ERR: use testdb: %v\n", err)
		t.FailNow()
	}

	if err := verifyAdaptive(ctx, conn, adaptiveWant()); err != nil {
		fmt.Printf("ERR: verify: %v\n", err)
		t.FailNow()
	}

	fmt.Println("OK")
}

func TestSleepCancel(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx, cancel := context.WithCancel(t.Context())
	var rows *sql.Rows
	var err error
	done := make(chan struct{})
	go func() {
		rows, err = conn.QueryContext(ctx, "select sleep(60)")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "select sleep(60) should return rows quickly.")
	}
	require.NoError(t, err)
	cancel()
	errCh := make(chan error)
	go func() {
		for rows.Next() {
		}
		errCh <- rows.Err()
	}()
	select {
	case err = <-errCh:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "canceling the query context should have made rows.Next() return quickly.")
	}
	require.Error(t, rows.Err())
	rows.Close()

	// Connection still works.
	r := conn.QueryRowContext(t.Context(), "select 3+4")
	require.NoError(t, r.Err())
	var i int
	require.NoError(t, r.Scan(&i))
	require.Equal(t, 7, i)

	// Canceling the context after the rows.Next call is blocked still unblocks it.
	ctx, cancel = context.WithCancel(t.Context())
	rows, err = conn.QueryContext(ctx, "select sleep(60)")
	require.NoError(t, err)
	var next bool
	done = make(chan struct{})
	go func() {
		next = rows.Next()
		close(done)
	}()
	select {
	case <-time.After(128 * time.Millisecond):
		// This is racey, but in general we are testing the case we want to here...
		cancel()
		select {
		case <-done:
		case <-time.After(128 * time.Millisecond):
			require.FailNow(t, "should have quickly finished after canceling the query context.")
		}
	case <-done:
		cancel()
		require.FailNow(t, "should not have finished rows.Next() until the context was canceled")
	}
	require.False(t, next)
	require.Error(t, rows.Err())
	rows.Close()
}

// TestNextResultSetSkipsExpensiveIteration verifies that calling NextResultSet() in the
// middle of a slow result set does not block waiting for remaining rows to be consumed.
// For example, "select 1 union select sleep(60); select 2" should allow advancing to
// the second result set without waiting 60 seconds for the sleep to complete.
func TestNextResultSetSkipsExpensiveIteration(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()

	// Wrap QueryContext in a goroutine in case the UNION is evaluated eagerly.
	var rows *sql.Rows
	var err error
	done := make(chan struct{})
	go func() {
		rows, err = conn.QueryContext(ctx, "select 1 union select sleep(60); select 2;")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "QueryContext should return quickly for a lazy iterator")
	}
	require.NoError(t, err)

	// Read the first row of the first result set.
	var v any
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 1, v)

	// Call NextResultSet() without consuming the rest of the first result set.
	// This should return quickly — it must not wait for sleep(60) to complete.
	nextDone := make(chan bool)
	go func() {
		nextDone <- rows.NextResultSet()
	}()
	select {
	case hasNext := <-nextDone:
		require.True(t, hasNext)
	case <-time.After(1 * time.Second):
		require.FailNow(t, "NextResultSet() must not wait for sleep(60) to complete")
	}

	// The second result set should be accessible.
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&v))
	require.EqualValues(t, 2, v)
	require.False(t, rows.Next())
	require.False(t, rows.NextResultSet())
	require.NoError(t, rows.Close())

	// Connection must still work after the abandoned sleep.
	r := conn.QueryRowContext(ctx, "select 3+4")
	var i int
	require.NoError(t, r.Scan(&i))
	require.Equal(t, 7, i)
}

// TestShowProcesslistAndKill tests the SHOW PROCESSLIST and KILL QUERY functionality.
// It verifies that active connections appear in the processlist with the correct info
// and command, and that a running query can be killed while both connections continue
// to work afterward.
func TestShowProcesslistAndKill(t *testing.T) {
	ctx := t.Context()

	dir := t.TempDir()

	// Step 1: Create database
	db, conn1 := initializeTestDatabaseConnectionAt(t, dir, false)
	t.Cleanup(func() {
		require.NoError(t, conn1.Close())
	})

	type entry struct {
		id      int64
		user    sql.NullString
		host    sql.NullString
		dbName  sql.NullString
		command sql.NullString
		state   sql.NullString
		info    sql.NullString
		tm      sql.NullInt64
	}
	scanEntries := func(rows *sql.Rows) []entry {
		res := make([]entry, 0)
		for rows.Next() {
			var e entry
			require.NoError(t, rows.Scan(&e.id, &e.user, &e.host, &e.dbName, &e.command, &e.tm, &e.state, &e.info))
			res = append(res, e)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		return res
	}

	// Step 2: Run SHOW PROCESSLIST on conn1 and assert that exactly one connection is present.
	rows, err := conn1.QueryContext(ctx, "show processlist")
	require.NoError(t, err)
	entries := scanEntries(rows)
	require.Len(t, entries, 1, "expected exactly one connection in processlist before conn2 is opened")

	// Step 3: Open a new, concurrent connection (conn2).
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)

	// Step 4: Run SELECT SLEEP(60) on conn2 concurrently.
	sleepErrCh := make(chan error, 1)
	go func() {
		_, sleepErr := conn2.ExecContext(ctx, "select sleep(60)")
		sleepErrCh <- sleepErr
	}()

	// Steps 5 & 6: Poll SHOW PROCESSLIST on conn1 until both connections appear,
	// then assert that the sleeping connection shows the correct state.
	var sleepConnID int64
	require.Eventually(t, func() bool {
		pRows, pErr := conn1.QueryContext(ctx, "show processlist")
		if pErr != nil {
			return false
		}
		newEntries := scanEntries(pRows)
		if len(newEntries) < 2 {
			return false
		}
		foundSleep := false
		for _, e := range newEntries {
			if e.info.Valid && strings.EqualFold(strings.TrimSpace(e.info.String), "select sleep(60)") {
				require.True(t, e.command.Valid)
				require.True(t, strings.EqualFold(strings.TrimSpace(e.command.String), "query"), "running queries are in command 'Query'")
				sleepConnID = e.id
				foundSleep = true
			}
		}
		return foundSleep
	}, 5*time.Second, 100*time.Millisecond, "expected both connections to appear in processlist with sleep query running")

	require.NotZero(t, sleepConnID, "expected to find the connection ID of the sleeping query")

	// Step 7: Kill the sleep query from conn1, leaving conn2's connection alive.
	_, err = conn1.ExecContext(ctx, fmt.Sprintf("kill query %d", sleepConnID))
	require.NoError(t, err)

	// Step 8: Assert the sleep query is killed in a timely manner.
	select {
	case sleepErr := <-sleepErrCh:
		require.Error(t, sleepErr, "expected an error after the sleep query was killed")
	case <-time.After(5 * time.Second):
		t.Fatal("sleep query was not killed within the expected timeout")
	}

	// Step 9: Assert both connections continue to work after the kill.
	row := conn1.QueryRowContext(ctx, "select 1")
	var v int
	require.NoError(t, row.Scan(&v))
	require.Equal(t, 1, v)

	row = conn2.QueryRowContext(ctx, "select 1")
	require.NoError(t, row.Scan(&v))
	require.Equal(t, 1, v)

	rows, err = conn1.QueryContext(ctx, "show full processlist")
	require.NoError(t, err)
	entries = scanEntries(rows)
	require.Len(t, entries, 2, "both connections show up in full processlist")
	hasCommandSleep := false
	for _, e := range entries {
		if e.command.Valid && strings.EqualFold(strings.TrimSpace(e.command.String), "sleep") {
			hasCommandSleep = true
		}
	}
	require.True(t, hasCommandSleep, "the processlist entry for conn2 is in Command = Sleep")

	require.NoError(t, conn2.Close())

	rows, err = conn1.QueryContext(ctx, "show full processlist")
	require.NoError(t, err)
	entries = scanEntries(rows)
	require.Len(t, entries, 1, "conn2 was removed from full processlist when it was closed")
}

func initializeTestDatabaseConnection(t *testing.T, clientFoundRows bool) (*sql.DB, *sql.Conn) {
	dir := t.TempDir()
	return initializeTestDatabaseConnectionAt(t, dir, clientFoundRows)
}

func openTestDatabaseConnectionAt(t *testing.T, dir string, clientFoundRows bool) *sql.DB {
	ctx := t.Context()
	query := url.Values{
		"commitname":      []string{"Billy Batson"},
		"commitemail":     []string{"shazam@gmail.com"},
		"database":        []string{"testdb"},
		"multistatements": []string{"true"},
	}
	if clientFoundRows {
		query["clientfoundrows"] = []string{"true"}
	}
	dsn := url.URL{Scheme: "file", Path: encodeDir(dir), RawQuery: query.Encode()}
	doltDB, err := sql.Open(DoltDriverName, dsn.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		doltDB.Close()
	})
	require.NoError(t, doltDB.PingContext(ctx))
	if runTestsAgainstMySQL {
		dsn := mysqlDsn
		if clientFoundRows {
			dsn += "&clientFoundRows=true"
		}
		mysqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			mysqlDB.Close()
		})
		require.NoError(t, mysqlDB.PingContext(ctx))
		return mysqlDB
	}
	return doltDB
}

func initializeTestDatabaseConnectionAt(t *testing.T, dir string, clientFoundRows bool) (*sql.DB, *sql.Conn) {
	ctx := t.Context()
	db := openTestDatabaseConnectionAt(t, dir, clientFoundRows)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	res, err := conn.ExecContext(ctx, "drop database if exists testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	res, err = conn.ExecContext(ctx, "create database testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	res, err = conn.ExecContext(ctx, "use testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	return db, conn
}

// requireResults uses |conn| to run the specified |query| and asserts that the results
// match |expected|. If any differences are encountered, the current test fails.
func requireResults(t *testing.T, conn *sql.Conn, query string, expected [][]any) {
	ctx := t.Context()
	vals := make([]any, len(expected[0]))

	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)

	for _, expectedRow := range expected {
		for i := range vals {
			vals[i] = &vals[i]
		}
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(vals...))
		for i, expectedVal := range expectedRow {
			require.EqualValues(t, expectedVal, vals[i])
		}
	}

	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

func encodeDir(dir string) string {
	// encodeDir translate a given path to a URL compatible path, mostly for windows compatibility
	if os.PathSeparator == '\\' {
		dir = strings.ReplaceAll(dir, `\`, `/`)
	}
	return dir
}

// TestTransactionCommit tests that transactions can be committed successfully and changes are persisted.
func TestTransactionCommit(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()

	// Create a test table
	_, err := conn.ExecContext(ctx, "CREATE TABLE tx_test (id int primary key, value varchar(256));")
	require.NoError(t, err)

	// Begin a transaction
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Insert data within the transaction
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (1, 'committed');")
	require.NoError(t, err)

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the data was persisted
	requireResults(t, conn, "SELECT * FROM tx_test;", [][]any{{1, "committed"}})
}

// TestTransactionRollback tests that transactions can be rolled back and changes are discarded.
func TestTransactionRollback(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()

	// Create a test table and insert initial data
	_, err := conn.ExecContext(ctx, "CREATE TABLE tx_test (id int primary key, value varchar(256));")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (1, 'initial');")
	require.NoError(t, err)

	// Begin a transaction
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Insert data within the transaction
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (2, 'rolled_back');")
	require.NoError(t, err)

	// Rollback the transaction
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify only the initial data exists (the insert was rolled back)
	requireResults(t, conn, "SELECT * FROM tx_test ORDER BY id;", [][]any{{1, "initial"}})
}

// TestTransactionMultipleOperations tests that multiple operations within a transaction
// are all committed or rolled back together.
func TestTransactionMultipleOperations(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()

	// Create a test table
	_, err := conn.ExecContext(ctx, "CREATE TABLE tx_test (id int primary key, value varchar(256));")
	require.NoError(t, err)

	// Test commit with multiple operations
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (1, 'first');")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (2, 'second');")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (3, 'third');")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	requireResults(t, conn, "SELECT * FROM tx_test ORDER BY id;",
		[][]any{{1, "first"}, {2, "second"}, {3, "third"}})

	// Test rollback with multiple operations
	tx, err = conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (4, 'fourth');")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "INSERT INTO tx_test VALUES (5, 'fifth');")
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	// Verify the rollback worked - should still only have the first 3 rows
	requireResults(t, conn, "SELECT * FROM tx_test ORDER BY id;",
		[][]any{{1, "first"}, {2, "second"}, {3, "third"}})
}

// TestBeginTxUnsupportedIsolationLevel tests that BeginTx returns an error for unsupported isolation levels.
func TestBeginTxUnsupportedIsolationLevel(t *testing.T) {
	_, conn := initializeTestDatabaseConnection(t, false)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	ctx := t.Context()

	// Try to begin a transaction with an unsupported isolation level
	_, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "isolation level not supported")
}

// Opening a Dolt database with the driver should always have its
// own copy of the files. Closing the database should close those
// files.
func TestOpenFDsAfterClose(t *testing.T) {
	if runTestsAgainstMySQL {
		t.SkipNow()
	}
	dir := t.TempDir()
	nomsDir, err := filepath.EvalSymlinks(dir)
	require.NoError(t, err)
	openFiles, supported, err := openFilesUnderDir(nomsDir)
	if !supported {
		t.SkipNow()
	}
	require.NoError(t, err)
	require.Empty(t, openFiles)

	t.Cleanup(func() {
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			openFiles, _, err := openFilesUnderDir(nomsDir)
			require.NoError(t, err)
			assert.Empty(t, openFiles)
		}, 1*time.Second, 50*time.Millisecond)
	})

	_, connOne := initializeTestDatabaseConnectionAt(t, nomsDir, false)
	t.Cleanup(func() {
		require.NoError(t, connOne.Close())
	})

	openFiles, _, err = openFilesUnderDir(nomsDir)
	require.NoError(t, err)
	require.NotEmpty(t, openFiles)
	numDbOne := len(openFiles)

	dbTwo := openTestDatabaseConnectionAt(t, nomsDir, false)

	openFiles, _, err = openFilesUnderDir(nomsDir)
	require.NoError(t, err)
	require.Greater(t, len(openFiles), numDbOne)

	// TODO: require.NoError after Close signals errors appropriately.
	dbTwo.Close()

	openFiles, _, err = openFilesUnderDir(nomsDir)
	require.NoError(t, err)

	// The way fslock is implemented, we can't actually currently cancel the LockWithTimeout syscall
	// which will have happened against the noms directory LOCK file. The LOCK file from the previous
	// database stays open until we close the holder.
	//
	// TODO: If we care, fix this. The only way I know of is Cgo and pthread_kill to EINTR the
	// blocked flock call. On Linux, Cgo and io_uring for the flock call might be another option.
	assert.True(t, len(openFiles) == numDbOne || len(openFiles) == (numDbOne+1), "We are left with at most one more open file.")
}

func TestBranchSelectedOnConnectionReuse(t *testing.T) {
	if runTestsAgainstMySQL {
		t.SkipNow()
	}
	db, conn := initializeTestDatabaseConnection(t, false)

	ctx := t.Context()

	// Read current branch name
	var originalBranch string
	rows, err := conn.QueryContext(ctx, "select active_branch()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&originalBranch))
	require.NoError(t, rows.Close())
	require.Equal(t, "main", originalBranch)

	// Check out a new branch.
	_, err = conn.ExecContext(ctx, "call dolt_checkout('-b', 'new_branch')")
	require.NoError(t, err)
	var newBranch string
	rows, err = conn.QueryContext(ctx, "select active_branch()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&newBranch))
	require.NoError(t, rows.Close())
	require.Equal(t, "new_branch", newBranch)

	require.NoError(t, conn.Close())

	conn, err = db.Conn(ctx)
	require.NoError(t, err)

	var secondConnBranch string
	rows, err = conn.QueryContext(ctx, "select active_branch()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&secondConnBranch))
	require.NoError(t, rows.Close())
	require.Equal(t, "main", secondConnBranch)

	require.NoError(t, conn.Close())
}

func TestDatabaseSelectedOnConnectionReuse(t *testing.T) {
	db, conn := initializeTestDatabaseConnection(t, false)

	ctx := t.Context()

	// Read current database name
	var originalDatabase string
	rows, err := conn.QueryContext(ctx, "select database()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&originalDatabase))
	require.NoError(t, rows.Close())
	require.Equal(t, "testdb", originalDatabase)

	// Check out a new branch.
	_, err = conn.ExecContext(ctx, "create database new_database")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "use new_database")
	require.NoError(t, err)
	var newDatabase string
	rows, err = conn.QueryContext(ctx, "select database()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&newDatabase))
	require.NoError(t, rows.Close())
	require.Equal(t, "new_database", newDatabase)

	require.NoError(t, conn.Close())

	conn, err = db.Conn(ctx)
	require.NoError(t, err)

	var secondConnDatabase string
	rows, err = conn.QueryContext(ctx, "select database()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&secondConnDatabase))
	require.NoError(t, rows.Close())
	require.Equal(t, "testdb", secondConnDatabase)

	require.NoError(t, conn.Close())
}

func TestMain(m *testing.M) {
	// disable metrics during test runs
	// no need to set it back to false since no test should have it set to true
	metricsDisabled.Store(true)
	os.Exit(m.Run())
}
