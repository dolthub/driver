package embedded

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

const doltEngineVersion = "0.40.17"

func openEmbeddedEngine(ctx context.Context, ds *DoltDataSource) (*engine.SqlEngine, *gmssql.Context, RetryPolicy, error) {
	var fs filesys.Filesys = filesys.LocalFS

	if ds == nil {
		return nil, nil, RetryPolicy{}, fmt.Errorf("nil datasource")
	}

	exists, isDir := fs.Exists(ds.Directory)
	if !exists {
		return nil, nil, RetryPolicy{}, fmt.Errorf("'%s' does not exist", ds.Directory)
	} else if !isDir {
		return nil, nil, RetryPolicy{}, fmt.Errorf("%s: is a file.  Need to specify a directory", ds.Directory)
	}

	fs, err := fs.WithWorkingDir(ds.Directory)
	if err != nil {
		return nil, nil, RetryPolicy{}, err
	}

	name := ds.Params[CommitNameParam]
	if name == nil {
		return nil, nil, RetryPolicy{}, fmt.Errorf("datasource must include %q", CommitNameParam)
	}
	email := ds.Params[CommitEmailParam]
	if email == nil {
		return nil, nil, RetryPolicy{}, fmt.Errorf("datasource must include %q", CommitEmailParam)
	}

	cfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  name[0],
		config.UserEmailKey: email[0],
	})

	mrEnv, err := loadMultiEnvFromDir(ctx, cfg, fs, ds.Directory, doltEngineVersion)
	if err != nil {
		return nil, nil, RetryPolicy{}, err
	}

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	// Thread DB-load parameters through Dolt's SQL engine so callers (embedded driver) can
	// influence storage open behavior (e.g. disable singleton cache, fail-fast on lock timeout).
	if ds.ParamIsTrue(NoCacheParam) || ds.ParamIsTrue(FailOnLockTimeoutParam) {
		seCfg.DBLoadParams = make(map[string]interface{}, 2)
		if ds.ParamIsTrue(NoCacheParam) {
			seCfg.DBLoadParams[dbfactory.DisableSingletonCacheParam] = struct{}{}
		}
		if ds.ParamIsTrue(FailOnLockTimeoutParam) {
			seCfg.DBLoadParams[dbfactory.FailOnJournalLockTimeoutParam] = struct{}{}
		}
	}

	se, err := engine.NewSqlEngine(ctx, mrEnv, seCfg)
	if err != nil {
		return nil, nil, RetryPolicy{}, err
	}

	gmsCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		_ = se.Close()
		return nil, nil, RetryPolicy{}, err
	}
	if database, ok := ds.Params[DatabaseParam]; ok && len(database) == 1 {
		gmsCtx.SetCurrentDatabase(database[0])
	}
	if ds.ParamIsTrue(ClientFoundRowsParam) {
		client := gmsCtx.Client()
		gmsCtx.SetClient(gmssql.Client{
			User:         client.User,
			Address:      client.Address,
			Capabilities: client.Capabilities | mysql.CapabilityClientFoundRows,
		})
	}

	rp, err := ParseRetryPolicy(ds)
	if err != nil {
		_ = se.Close()
		return nil, nil, RetryPolicy{}, err
	}

	return se, gmsCtx, rp, nil
}

func loadMultiEnvFromDir(
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

