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

func init() {
	sql.Register("dolt", &DoltDriver{})
}

type DoltDriver struct {
}

func (d *DoltDriver) Open(dataSource string) (driver.Conn, error) {
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

	username := ds.Params["username"]
	if username == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter 'username'", dataSource)
	}

	email := ds.Params["email"]
	if email == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter 'email'", dataSource)
	}

	cfg := config.NewMapConfig(map[string]string{
		env.UserNameKey:  username[0],
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

	if database, ok := ds.Params["database"]; ok && len(database) == 1 {
		seCfg.InitialDb = database[0]
	}

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatNull, seCfg)
	if err != nil {
		return nil, err
	}

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	sqlCtx.Session.SetClient(gms.Client{User: "root", Address: "%", Capabilities: 0})
	return &DoltConn{
		se:     se,
		SqlCtx: sqlCtx,
	}, nil
}
