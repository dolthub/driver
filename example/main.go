package main

import (
	"database/sql"
	"fmt"
	_ "github.com/dolthub/embedded"
	"os"
)

func errExit(wrapFormat string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, fmt.Errorf(wrapFormat, err).Error())
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: example file:///path/to/doltdb?username=<user_name>&email=<email>")
		return
	}

	dataSource := os.Args[1]
	fmt.Println("Connecting to", dataSource)

	db, err := sql.Open("dolt", dataSource)
	errExit("failed to open database using the dolt driver: %w", err)

	rows, err := db.Query("SHOW DATABASES;")
	errExit("SHOW DATABASES query failed: %w", err)

	printCols(rows)

	var dbName string
	for rows.Next() {
		err = rows.Scan(&dbName)
		errExit("scan failed: %w", err)
		fmt.Println(dbName)
	}
}

func printCols(rows *sql.Rows) {
	cols, err := rows.Columns()
	errExit("Failed to get columns: %w", err)

	colsStr := fmt.Sprint(cols)
	separatorBytes := make([]byte, len(colsStr))
	for i := range separatorBytes {
		separatorBytes[i] = byte('-')
	}

	fmt.Println(colsStr)
	fmt.Println(string(separatorBytes))
}
