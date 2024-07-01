package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/go-sql-driver/mysql"

	_ "github.com/dolthub/driver"
)

func main() {
	ctx := context.Background()
	if ctx == nil {}

	dir := "./"
	params := url.Values{
		"database": []string{"tmp"},
		"commitname": []string{"root"},
		"commitemail": []string{"root@dolthub.com"},
		"multistatements": []string{"true"},
	}
	dsn := url.URL{Scheme: "file", Path: dir, RawQuery: params.Encode()}
	db, err := sql.Open("dolt", dsn.String())
	if err != nil {
		panic(err)
	}
	db.Ping()

	cfg := mysql.Config{
		User:   "root",
		Passwd: "root",
		Net:    "tcp",
		Addr:   "127.0.0.1:3306",
		DBName: "test_db",
		AllowNativePasswords: true,
	}
	if cfg.Addr == "" {}
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		panic(err)
	}
	db.Ping()

	defer db.Close()

	//var res sql.Result
	//res, err = db.ExecContext(ctx, "select 1; select 2; select 3;")
	//if err != nil {
	//	panic(err)
	//}
	//numRows, err := res.RowsAffected()
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Sprintf("Rows Affected: %d", numRows)

	var rows *sql.Rows
	rows, err = db.QueryContext(ctx, "select 1;")
	if err != nil {
		panic(err)
	}
	var s int
	for {
		fmt.Printf("Result Set #%d\n", s)
		for rows.Next() {
			var i int
			err = rows.Scan(&i)
			if err != nil {
				panic(err)
			}
			println(i)
		}
		s++

		if !rows.NextResultSet() {
			break
		}
	}

	//defer db.ExecContext(ctx, "drop procedure p")
	return

	//cfg := mysql.Config{
	//	User:   "root",
	//	Passwd: "",
	//	Net:    "tcp",
	//	Addr:   "127.0.0.1:3306",
	//	DBName: "tmp",
	//	AllowNativePasswords: true,
	//}
 	//// Get a database handle.
    //db, err = sql.Open("mysql", cfg.FormatDSN())
    //if err != nil {
	//	panic(err)
    //}
	//defer db.Close()
	//
	//var rows *sql.Rows
	//rows, err = db.QueryContext(ctx, "delimiter //")
	//if err != nil {
	//	panic(err)
	//}
	//for rows.Next() {
	//	var i int
	//	err = rows.Scan(&i)
	//	if err != nil {
	//		panic(err)
	//	}
	//	fmt.Println(i)
	//}
}