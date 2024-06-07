
# Dolt Database Driver

This package provides a driver to be used with the database/sql package for database access in Golang.
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

Finally you can create the dbs directory as shown above and then create the database in code using a SQL `CREATE TABLE` statement

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
