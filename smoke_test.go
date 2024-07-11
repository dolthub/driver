package embedded

import (
	"context"
	"database/sql"
	"net/url"
	"os"
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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

	require.NoError(t, conn.Close())
}

// TestMultiStatementsExecContext tests that using ExecContext to run a multi-statement query works as expected and
// matches the behavior of the MySQL driver.
func TestMultiStatementsExecContext(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	// QueryContext returns the results from the FIRST statement executed. This differs from the behavior for ExecContext.
	ctx := context.Background()
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	var v int
	ctx := context.Background()
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	var v int
	ctx := context.Background()

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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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
			conn, cleanupFunc := initializeTestDatabaseConnection(t, test.clientFoundRows)
			defer cleanupFunc()
			ctx := context.Background()

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
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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

	require.NoError(t, conn.Close())
}

// TestTypes asserts that various MySQL types are returned as the expected Go type by the driver.
func TestTypes(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
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

// initializeTestDatabaseConnection create a test database called testdb and initialize a database/sql connection
// using the Dolt driver. The connection, |conn|, is returned, and |cleanupFunc| is a function that the test function
// should defer in order to properly dispose of test resources.
func initializeTestDatabaseConnection(t *testing.T, clientFoundRows bool) (conn *sql.Conn, cleanUpFunc func()) {
	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)

	cleanUpFunc = func() {
		os.RemoveAll(dir)
	}

	ctx := context.Background()

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
	db, err := sql.Open(DoltDriverName, dsn.String())
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	if runTestsAgainstMySQL {
		dsn := mysqlDsn
		if clientFoundRows {
			dsn += "&clientFoundRows=true"
		}
		db, err = sql.Open("mysql", dsn)
		require.NoError(t, err)
		require.NoError(t, db.PingContext(ctx))
	}

	conn, err = db.Conn(ctx)
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

	return conn, cleanUpFunc
}

// requireResults uses |conn| to run the specified |query| and asserts that the results
// match |expected|. If any differences are encountered, the current test fails.
func requireResults(t *testing.T, conn *sql.Conn, query string, expected [][]any) {
	ctx := context.Background()
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
