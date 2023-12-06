package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	DoltDriverName = "dolt"

	CommitNameParam      = "commitname"
	CommitEmailParam     = "commitemail"
	DatabaseParam        = "database"
	MultiStatementsParam = "multistatements"
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
//
//	file:///User/brian/driver/example/path?commitname=Billy%20Bob&commitemail=bb@gmail.com&database=dbname
//
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
		config.UserNameKey:  name[0],
		config.UserEmailKey: email[0],
	})

	mrEnv, err := LoadMultiEnvFromDir(ctx, cfg, fs, ds.Directory, "0.40.17")
	if err != nil {
		return nil, err
	}

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}
	
	se, err := engine.NewSqlEngine(ctx, mrEnv, seCfg)
	if err != nil {
		return nil, err
	}

	gmsCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	if database, ok := ds.Params[DatabaseParam]; ok && len(database) == 1 {
		gmsCtx.SetCurrentDatabase(database[0])
	}

	return &DoltConn{
		DataSource: ds,
		se:         se,
		gmsCtx:     gmsCtx,
	}, nil
}

// LoadMultiEnvFromDir looks at each subfolder of the given path as a Dolt repository and attempts to return a MultiRepoEnv
// with initialized environments for each of those subfolder data repositories. subfolders whose name starts with '.' are
// skipped.
func LoadMultiEnvFromDir(
		ctx context.Context,
		cfg config.ReadWriteConfig,
		fs filesys.Filesys,
		path, version string,
) (*env.MultiRepoEnv, error) {

	multiDbDirFs, err := fs.WithWorkingDir(path)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}
	
	return env.MultiEnvForDirectory(ctx, cfg, multiDbDirFs, version, nil)
}
