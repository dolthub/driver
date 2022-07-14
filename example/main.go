package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/embedded"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"os"
)

func errExit(wrapFormat string, err error) {
	if err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, fmt.Errorf(wrapFormat, err).Error())
		os.Exit(1)
	}
}

func main() {
	dir := os.Args[1]
	ctx := context.Background()

	dolt, err := embedded.NewEmbeddedDolt(ctx, filesys.LocalFS, dir, "Embedded User", "embedded@fake.horse")
	errExit("failed to create embedded dolt instance: %w", err)
	defer func() {
		dolt.Close()
	}()

	rowCount := 0
	countCallback := func(sch sql.Schema, rowIdx int, row sql.Row) error {
		if rowIdx != 0 {
			return errors.New("only a single row was expected but received multiple")
		}

		rowCount = int(row[0].(int64))
		return nil
	}

	query(dolt, "CREATE DATABASE IF NOT EXISTS test;", nil)
	query(dolt, "USE test;", nil)
	query(dolt, "CREATE TABLE IF NOT EXISTS t1 (pk int primary key, c1 varchar(512));", nil)
	query(dolt, "SELECT count(*) FROM t1;", countCallback)

	if rowCount == 0 {
		query(dolt, "INSERT INTO t1 VALUES (1, 'row 1'), (2, 'row 2'), (3, 'row 3');", nil)
	}

	fmt.Println("pk, c1")
	rowCallback := func(sch sql.Schema, rowIdx int, row sql.Row) error {
		fmt.Printf("% 2d, %s\n", row[0].(int32), row[1].(string))
		return nil
	}
	query(dolt, "SELECT * FROM t1 WHERE pk > 1", rowCallback)
}

func query(dolt *embedded.EmbeddedDolt, query string, cb func(sch sql.Schema, rowIdx int, row sql.Row) error) {
	sch, ri, err := dolt.Query(query)
	errExit(fmt.Sprintf("Failed to execute query '%s': %%w", query), err)
	defer func() {
		err := ri.Close(dolt.SqlCtx)
		errExit("Failed to close row iterator: %w", err)
	}()

	rowIdx := 0
	next, err := ri.Next(dolt.SqlCtx)
	errExit("Failed getting next now: %w", err)

	for ; err == nil; rowIdx++ {
		if cb != nil {
			err := cb(sch, rowIdx, next)
			errExit("failed processing row: %w", err)
		}

		next, err = ri.Next(dolt.SqlCtx)
		errExit("Failed getting next row: %w", err)
	}
}
