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
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiDBLockContention verifies that when one connection holds the write lock on a
// subdirectory database, a second connection opening the parent directory (which discovers
// and tries to open the same subdirectory) returns an error instead of panicking.
func TestMultiDBLockContention(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows: file handle cleanup is racey with t.TempDir()")
	}

	rootDir, err := os.MkdirTemp("", "TestMultiDBLockContention")
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	ctx := context.Background()

	baseQuery := url.Values{
		"commitname":  []string{"test"},
		"commitemail": []string{"test@test.com"},
	}

	// First, open the root directory and create a "first" database so that
	// a .dolt directory exists at rootDir/first.
	rootDSN := url.URL{Scheme: "file", Path: encodeDir(rootDir), RawQuery: baseQuery.Encode()}
	rootCfg, err := ParseDSN(rootDSN.String())
	require.NoError(t, err)
	rootConnector, err := NewConnector(rootCfg)
	require.NoError(t, err)
	setupDB := sql.OpenDB(rootConnector)
	conn, err := setupDB.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE DATABASE `firstdb`")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	_ = rootConnector.Close()

	// Now open the subdirectory database directly — this holds the journal/write lock.
	subDir := filepath.Join(rootDir, "firstdb")
	subDSN := url.URL{Scheme: "file", Path: encodeDir(subDir), RawQuery: baseQuery.Encode()}
	subCfg, err := ParseDSN(subDSN.String())
	require.NoError(t, err)
	subConnector, err := NewConnector(subCfg)
	require.NoError(t, err)
	subDB := sql.OpenDB(subConnector)
	require.NoError(t, subDB.PingContext(ctx))

	// Now open the root directory again with a bounded backoff. It will discover
	// the "firstdb" subdirectory and fail to acquire its lock. This must return an
	// error, not panic.
	rootCfg2, err := ParseDSN(rootDSN.String())
	require.NoError(t, err)
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 2 * time.Second
	bo.MaxInterval = 100 * time.Millisecond
	rootCfg2.BackOff = bo
	rootConnector2, err := NewConnector(rootCfg2)
	require.NoError(t, err)
	rootDB2 := sql.OpenDB(rootConnector2)

	err = rootDB2.PingContext(ctx)
	assert.Error(t, err, "expected an error due to lock contention, not a panic")

	rootDB2.Close()
	_ = rootConnector2.Close()
	subDB.Close()
	_ = subConnector.Close()
}
