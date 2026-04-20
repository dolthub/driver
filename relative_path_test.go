// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestLoadMultiEnvFromDirRelativePath verifies that LoadMultiEnvFromDir works
// correctly when the filesystem is already rooted at the target directory and
// the path argument is ".". This is the core of the relative-path fix: the
// connector roots the filesystem at cfg.Directory, so LoadMultiEnvFromDir
// must not apply that directory a second time.
func TestLoadMultiEnvFromDirRelativePath(t *testing.T) {
	// Create <tmp>/data/myapp to simulate a database directory.
	base := t.TempDir()
	dbDir := filepath.Join(base, "data", "myapp")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	cfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  "test",
		config.UserEmailKey: "test@test.com",
	})

	// Simulate what the connector does: root the filesystem at the target dir.
	fs, err := filesys.LocalFilesysWithWorkingDir(dbDir)
	require.NoError(t, err)

	// With the fix, openSqlEngine passes "." since fs is already at dbDir.
	_, err = LoadMultiEnvFromDir(context.Background(), cfg, fs, ".", "0.40.17", nil)
	require.NoError(t, err)

	// Before the fix, the relative path would be applied again, doubling it.
	// This should fail because <dbDir>/data/myapp does not exist.
	_, err = LoadMultiEnvFromDir(context.Background(), cfg, fs, "data/myapp", "0.40.17", nil)
	require.Error(t, err, "applying the relative directory twice should fail because the doubled path does not exist")
}

// TestConnectorRelativePathPassesDot verifies that the connector, when given a
// relative-path DSN, passes "." as the directory to openSqlEngine (via the fix
// in openSqlEngine that replaces dir with "."). We mock the engine open and
// verify that LoadMultiEnvFromDir succeeds with the captured fs and ".".
func TestConnectorRelativePathPassesDot(t *testing.T) {
	// Create <tmp>/data/myapp.
	base := t.TempDir()
	dbDir := filepath.Join(base, "data", "myapp")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	// chdir so that the relative path "./data/myapp" resolves correctly.
	origWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(base))
	t.Cleanup(func() { os.Chdir(origWd) })

	// Capture the fs that the connector builds after WithWorkingDir.
	var capturedFs filesys.Filesys
	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(_ context.Context, _ config.ReadWriteConfig, fs filesys.Filesys, dir, _ string, _ *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		capturedFs = fs
		return &engine.SqlEngine{}, nil
	}

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(_ *engine.SqlEngine, _ context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dsn := "file://./data/myapp?commitname=Test&commitemail=test@example.com"
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	c, err := NewConnector(cfg)
	require.NoError(t, err)
	_, err = c.Connect(context.Background())
	require.NoError(t, err)

	require.NotNil(t, capturedFs)

	doltCfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  "Test",
		config.UserEmailKey: "test@example.com",
	})

	// The connector roots fs at ./data/myapp. With the fix, openSqlEngine
	// passes "." to LoadMultiEnvFromDir, which should work.
	_, err = LoadMultiEnvFromDir(context.Background(), doltCfg, capturedFs, ".", "0.40.17", nil)
	require.NoError(t, err)

	// Without the fix, the original dir ("./data/myapp") would be passed
	// again, doubling the path to ./data/myapp/data/myapp which doesn't exist.
	_, err = LoadMultiEnvFromDir(context.Background(), doltCfg, capturedFs, cfg.Directory, "0.40.17", nil)
	require.Error(t, err, "re-applying the relative directory should fail because the doubled path does not exist")

	require.NoError(t, c.Close())
}
