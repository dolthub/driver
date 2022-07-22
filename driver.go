package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	gms "github.com/dolthub/go-mysql-server/sql"
)

const (
	DoltDriverName = "dolt"

	CommitNameParam  = "commitname"
	CommitEmailParam = "commitemail"
	DatabaseParam    = "database"
)

var _ driver.Driver = (*doltDriver)(nil)

func init() {
	sql.Register(DoltDriverName, &doltDriver{})
}

// doltDriver is a driver.Driver implementation which provides access to a dolt database on the local filesystem
type doltDriver struct {
}

// Open opens and returns a connection to the datasource referenced by the string provided using the options provided.
// datasources must be in file url format:
//   file:///User/brian/driver/example/path?commitname=Billy%20Bob&commitemail=bb@gmail.com&database=dbname
// The path needs to point to a directory whose subdirectories are dolt databases.  If a "Create Database" command is
// run a new subdirectory will be created in this path.
// The supported parameters are
func (d *doltDriver) Open(dataSource string) (driver.Conn, error) {
	ctx := context.Background()
	var fs filesys.Filesys = filesys.LocalFS

	ds, err := ParseDataSource(dataSource)
	if err != nil {
		return nil, err
	}

	exists, isDir := fs.Exists(ds.Directory)
	if !exists {
		return nil, fmt.Errorf("'%s' does not exist", ds.Directory)
	} else if !isDir {
		return nil, fmt.Errorf("%s: is a file.  Need to specify a directory", ds.Directory)
	}

	fs, err = fs.WithWorkingDir(ds.Directory)
	if err != nil {
		return nil, err
	}

	name := ds.Params[CommitNameParam]
	if name == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dataSource, CommitNameParam)
	}

	email := ds.Params[CommitEmailParam]
	if email == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dataSource, CommitEmailParam)
	}

	cfg := config.NewMapConfig(map[string]string{
		env.UserNameKey:  name[0],
		env.UserEmailKey: email[0],
	})

	//mrEnv, err := env.MultiEnvForDirectory(ctx, cfg, fs, "0.40.17", true)
	mrEnv, err := env.LoadMultiEnvFromDir(ctx, env.GetCurrentUserHomeDir, cfg, fs, ds.Directory, "0.40.17", true)
	if err != nil {
		return nil, err
	}

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	if database, ok := ds.Params[DatabaseParam]; ok && len(database) == 1 {
		seCfg.InitialDb = database[0]
	}

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatNull, seCfg)
	if err != nil {
		return nil, err
	}

	gmsCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	gmsCtx.Session.SetClient(gms.Client{User: "root", Address: "%", Capabilities: 0})
	return &DoltConn{
		DataSource: ds,
		se:         se,
		gmsCtx:     gmsCtx,
	}, nil
}
