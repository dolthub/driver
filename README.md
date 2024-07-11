
# Dolt Database Driver

This package provides a `database/sql` compatible driver for [embedding Dolt inside a Go application](https://www.dolthub.com/blog/2022-07-25-embedded/).
It allows you to access local [Dolt](https://github.com/dolthub/dolt) databases via the file system, akin to SQLite, without running a Dolt server process.

For details of the database/sql package see [this tutorial](https://go.dev/doc/tutorial/database-access).
Below I will cover things that are specific to using dolt with this database driver.

### Ways to Set up a Dolt Database

Create a directory to house one or more dolt databases.  Once this is created we'll create a directory for our first
database using the [dolt cli](https://doltdb.com)

```bash
mkdir dbs
mkdir dbs/testdb
cd dbs/testdb
dolt init
```

Alternatively you could clone a database from dolthub or a different remote:

```bash
mkdir dbs
cd dbs
dolt clone <REMOTE URL>
```

Finally, you can create the dbs directory as shown above and then create the database in code using a SQL `CREATE TABLE` statement

### Connecting to the Database

First we'll import the dolt driver so that it will be registered

```go
_ "github.com/dolthub/driver"
```

Then we will open a connection to the database:

```go
db, err := sql.Open("dolt", "file:///path/to/dbs?commitname=Your%20Name&commitemail=your@email.com&database=databasename")
```

Now you can use your `db` as you would normally, however you have access to all of dolt's special features as well. 

### Dolt Data Source Names

The Dolt driver requires a DSN containing the directory where your databases live, and the name and email that are used in
the commit log.

```
commitname - The name of the committer seen in the dolt commit log
commitemail - The email of the committer seen in the dolt commit log
database - The initial database to connect to
multistatements - If set to true, allows multiple statements in one query
clientfoundrows - If set to true, returns the number of matching rows instead of the number of changed rows in UPDATE queries
```

#### Example DSN

`file:///path/to/dbs?commitname=Your%20Name&commitemail=your@email.com&database=databasename`

### Multi-Statement Support

If you pass the `multistatements=true` parameter in the DSN, you can execute multiple statements in one query. The returned 
rows allow you to iterate over the returned result sets by using the `NextResultSet` method, just like you can with the
MySQL driver. 

```go
rows, err := db.Query("SELECT * from someTable; SELECT * from anotherTable;")
// If an error is returned, it means it came from the first statement
if err != nil {
	panic(err)
}

for rows.Next() {
	// process the first result set
}

if rows.NextResultSet() {
    for rows.Next() {
        // process the second result set
    }
} else {
	// If NextResultSet returns false when there were more statements, it means there was an error,
	// which you can access through rows.Err()
	panic(rows.Err())
}
```
