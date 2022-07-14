package embedded

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/go-mysql-server/sql"
	"os"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

type EmbeddedDolt struct {
	se     *engine.SqlEngine
	SqlCtx *sql.Context
}

func NewEmbeddedDolt(ctx context.Context, fs filesys.Filesys, directory, username, email string) (*EmbeddedDolt, error) {
	exists, isDir := fs.Exists(directory)
	if !exists {
		err := os.MkdirAll(directory, os.ModePerm)
		if err != nil {
			return nil, err
		}
	} else if !isDir {
		return nil, fmt.Errorf("%s: is a file.  Need to specify a directory", directory)
	}

	var err error
	fs, err = fs.WithWorkingDir(directory)
	if err != nil {
		return nil, err
	}

	cfg := config.NewMapConfig(map[string]string{
		env.UserNameKey:  username,
		env.UserEmailKey: email,
	})
	mrEnv, err := env.MultiEnvForDirectory(ctx, cfg, fs, "0.40.17", true)
	if err != nil {
		return nil, err
	}

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatNull, seCfg)
	if err != nil {
		return nil, err
	}

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	sqlCtx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})
	return &EmbeddedDolt{
		se:     se,
		SqlCtx: sqlCtx,
	}, nil
}

func (ed *EmbeddedDolt) Query(query string) (sql.Schema, sql.RowIter, error) {
	return ed.se.Query(ed.SqlCtx, query)
}

func (ed *EmbeddedDolt) Close() error {
	return ed.se.Close()
}
