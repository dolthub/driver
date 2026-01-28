package embedded

import (
	"context"
	"fmt"
	"maps"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
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

	// Build DB load params early so they apply during env / DB discovery.
	dbLoadParams := make(map[string]interface{}, 2)
	if ds.ParamIsTrue(NoCacheParam) {
		dbLoadParams[dbfactory.DisableSingletonCacheParam] = struct{}{}
	}
	if ds.ParamIsTrue(FailOnLockTimeoutParam) {
		dbLoadParams[dbfactory.FailOnJournalLockTimeoutParam] = struct{}{}
	}
	if len(dbLoadParams) == 0 {
		dbLoadParams = nil
	}

	mrEnv, err := loadMultiEnvFromDir(ctx, cfg, fs, doltEngineVersion, dbLoadParams)
	if err != nil {
		return nil, nil, RetryPolicy{}, err
	}

	// Ensure all discovered DBs are actually loadable before we construct the SQL engine.
	// Without this, a lock-timeout failure can result in a nil *doltdb.DoltDB that later panics
	// during engine construction (DbEaFactory deref).
	if err := ensureMultiRepoEnvLoaded(ctx, mrEnv); err != nil {
		return nil, nil, RetryPolicy{}, err
	}

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	// Thread DB-load parameters through Dolt's SQL engine so callers (embedded driver) can
	// influence storage open behavior (e.g. disable singleton cache, fail-fast on lock timeout).
	if dbLoadParams != nil {
		seCfg.DBLoadParams = maps.Clone(dbLoadParams)
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

func ensureMultiRepoEnvLoaded(ctx context.Context, mrEnv *env.MultiRepoEnv) error {
	if mrEnv == nil {
		return fmt.Errorf("nil MultiRepoEnv")
	}
	return mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		if dEnv == nil {
			return false, nil
		}
		if dEnv.DoltDB(ctx) == nil {
			if dEnv.DBLoadError != nil {
				return true, dEnv.DBLoadError
			}
			return true, fmt.Errorf("failed to load database %q", name)
		}
		return false, nil
	})
}

func loadMultiEnvFromDir(
	ctx context.Context,
	cfg config.ReadWriteConfig,
	fs filesys.Filesys,
	version string,
	dbLoadParams map[string]interface{},
) (*env.MultiRepoEnv, error) {
	// Create a root env with DBLoadParams so MultiEnvForDirectory will apply them before it eagerly loads DBs.
	rootEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, version)
	if rootEnv != nil && dbLoadParams != nil {
		rootEnv.DBLoadParams = maps.Clone(dbLoadParams)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, cfg, fs, version, rootEnv)
	if err != nil {
		return nil, err
	}

	// Engine/provider close is expected to release all underlying storage.
	return mrEnv, nil
}
