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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenLoadsPersistedVariables(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := Config{Directory: dir, CommitName: "test", CommitEmail: "test@test.com"}

	connector, err := NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	_, err = db.ExecContext(ctx, "CREATE DATABASE persisted")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, connector.Close())

	open := func() (*sql.DB, *Connector) {
		connector, err := NewConnector(cfg)
		require.NoError(t, err)
		return sql.OpenDB(connector), connector
	}

	writePersistedGlobal(t, filepath.Join(dir, "persisted"), "max_connections", "321")
	db, connector = open()
	var maxConnections int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT @@global.max_connections").Scan(&maxConnections))
	require.Equal(t, int64(321), maxConnections)
	require.NoError(t, db.Close())
	require.NoError(t, connector.Close())

	writePersistedGlobal(t, filepath.Join(dir, "persisted"), "max_connections", "not a number")
	db, connector = open()
	defer connector.Close()
	defer db.Close()
	require.ErrorContains(t, db.PingContext(ctx), "max_connections")
}

func writePersistedGlobal(t *testing.T, dbDir, name, value string) {
	path := filepath.Join(dbDir, ".dolt", "config.json")
	values := map[string]string{}
	if data, err := os.ReadFile(path); err == nil {
		require.NoError(t, json.Unmarshal(data, &values))
	} else {
		require.True(t, os.IsNotExist(err))
	}
	values["sqlserver.global."+name] = value
	data, err := json.Marshal(values)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0644))
}
