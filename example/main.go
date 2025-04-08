package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/dolthub/driver"
)

func errExit(wrapFormat string, err error) {
	if err != nil {
		if len(wrapFormat) > 0 {
			err = fmt.Errorf(wrapFormat, err)
		}

		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: example file:///path/to/doltdb?commitname=<user_name>&commitemail=<email>&database=<database>&multistatements=<true|false>")
		return
	}

	ctx := context.Background()

	dataSource := os.Args[1]
	fmt.Println("Connecting to", dataSource)
	db, err := sql.Open("dolt", dataSource)
	errExit("failed to open database using the dolt driver: %w", err)

	err = printQuery(ctx, db, "CREATE DATABASE IF NOT EXISTS testdb; USE testdb;")
	errExit("", err)

	err = printQuery(ctx, db, "USE testdb;")
	errExit("", err)

	err = printQuery(ctx, db, `CREATE TABLE IF NOT EXISTS t2(
		pk int primary key auto_increment,
		c1 varchar(32)
	)`)
	errExit("", err)

	printQuery(ctx, db, "SHOW TABLES;")

	stmt, err := db.PrepareContext(ctx, "Insert into t2 (c1) values (?);")
	errExit("", err)
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx, "test")
	errExit("", err)

	fmt.Println(result.LastInsertId())

	err = printQuery(ctx, db, `CREATE TABLE IF NOT EXISTS t1 (
		pk int PRIMARY KEY,
		c1 varchar(512),
		c2 float,
		c3 bool,
		c4 datetime
	);`)
	errExit("", err)

	err = printQuery(ctx, db, "SELECT * FROM t1;")
	errExit("", err)

	err = printQuery(ctx, db, `REPLACE INTO t1 VALUES 
		(1, 'this is a test', 0, 0, '1998-01-23 12:45:56'),
		(2, 'it is only a test', 1.0, 1, '2010-12-31 01:15:00'),
		(3, NULL, 3.335, 0, NULL),
		(4, 'something something', 3.5, 1, '2015-04-03 14:00:45');`)
	errExit("", err)

	err = printQuery(ctx, db, "SELECT * FROM t1;")
	errExit("", err)

	err = printQuery(ctx, db, "DELETE FROM t1;")
	errExit("", err)

	rows := [][]interface{}{
		{1, "this is a test", 0, 0, time.Date(1998, 1, 23, 12, 45, 56, 0, time.UTC)},
		{2, "it is only a test", 1.0, 1, time.Date(2010, 12, 31, 1, 15, 0, 0, time.UTC)},
		{3, nil, 3.335, 0, nil},
		{4, "something something", 3.5, 1, time.Date(2015, 4, 3, 14, 0, 45, 0, time.UTC)},
	}

	tx, err := db.Begin()
	errExit("", err)

	err = prepareAndExec(ctx, tx, "INSERT INTO t1 VALUES (?, ?, ?, ?, ?)", rows)
	errExit("", err)

	err = printQuery(ctx, tx, `INSERT INTO t1 VALUES (5, "blah", 4.0, 0, now());
INSERT INTO t1 VALUES (6, 'aoeu', 7.0, 1, now()), (7,"aoeu aoeu", 8.1, 0, now());
SELECT * FROM t1;`)

	fmt.Println("Query Before Rollback")
	err = printQuery(ctx, tx, "SELECT * FROM t1;")

	//err = tx.Rollback()
	//errExit("", err)
	//fmt.Println("Query After Rollback")

	err = tx.Commit()
	errExit("", err)
	fmt.Println("Query After Commit")

	err = printQuery(ctx, db, "SELECT * FROM t1;")
	errExit("", err)
}

type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func printQuery(ctx context.Context, queryable Queryable, query string) error {
	fmt.Println("query:", query)
	rows, err := queryable.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query '%s' failed: %w", query, err)
	}
	defer rows.Close()

	fmt.Println("results:")
	err = printRows(rows)
	if err != nil {
		return err
	}

	fmt.Println()
	return nil
}

func printRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Failed to get columns: %w", err)
	}

	fmt.Println(strings.Join(cols, "|"))

	for rows.Next() {
		values := make([]interface{}, len(cols))
		var generic = reflect.TypeOf(values).Elem()
		for i := 0; i < len(cols); i++ {
			values[i] = reflect.New(generic).Interface()
		}

		err = rows.Scan(values...)
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		result := bytes.NewBuffer(nil)
		for i := 0; i < len(cols); i++ {
			if i != 0 {
				result.WriteString("|")
			}

			var rawValue = *(values[i].(*interface{}))
			switch val := rawValue.(type) {
			case string:
				result.WriteString(val)
			case int:
				result.WriteString(strconv.FormatInt(int64(val), 10))
			case int8:
				result.WriteString(strconv.FormatInt(int64(val), 10))
			case int16:
				result.WriteString(strconv.FormatInt(int64(val), 10))
			case int32:
				result.WriteString(strconv.FormatInt(int64(val), 10))
			case int64:
				result.WriteString(strconv.FormatInt(val, 10))
			case uint:
				result.WriteString(strconv.FormatUint(uint64(val), 10))
			case uint8:
				result.WriteString(strconv.FormatUint(uint64(val), 10))
			case uint16:
				result.WriteString(strconv.FormatUint(uint64(val), 10))
			case uint32:
				result.WriteString(strconv.FormatUint(uint64(val), 10))
			case uint64:
				result.WriteString(strconv.FormatUint(val, 10))
			case float32:
				result.WriteString(strconv.FormatFloat(float64(val), 'f', 2, 64))
			case float64:
				result.WriteString(strconv.FormatFloat(val, 'f', 2, 64))
			case bool:
				if val {
					result.WriteString("true")
				} else {
					result.WriteString("false")
				}
			case []byte:
				enc := base64.NewEncoder(base64.URLEncoding, result)
				_, err := enc.Write(val)
				errExit("failed to base64 encode blob: %w", err)
			case time.Time:
				timeStr := val.Format(time.RFC3339)
				result.WriteString(timeStr)
			}
		}

		fmt.Println(result.String())
	}

	return nil
}

type Preparable interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

func prepareAndExec(ctx context.Context, prepable Preparable, query string, vals [][]interface{}) error {
	stmt, err := prepable.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	for i := range vals {
		result, err := stmt.ExecContext(ctx, vals[i]...)
		if err != nil {
			return fmt.Errorf("failed to execute prepared statement '%s' with parameters: %v - %w", query, vals[i], err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get num rows affected: %w", err)
		}

		if affected != 1 {
			return fmt.Errorf("expected '%s' to affect 1 row but it affected %d; params: %v - %w", query, affected, vals[i], err)
		}
	}

	return nil
}
