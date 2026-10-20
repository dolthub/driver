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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorEngineVariableIsolation(t *testing.T) {
	ctx := context.Background()
	open := func() (*Connector, *sql.DB) {
		connector, err := NewConnector(Config{
			Directory:   t.TempDir(),
			CommitName:  "test",
			CommitEmail: "test@test.com",
		})
		require.NoError(t, err)
		db := sql.OpenDB(connector)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
			require.NoError(t, connector.Close())
		})
		return connector, db
	}
	getInt := func(conn *sql.Conn, query string) int64 {
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&value))
		return value
	}

	a, dbA := open()
	b, dbB := open()
	var conns [2]*sql.Conn
	var openErrs [2]error
	var wg sync.WaitGroup
	for i, db := range []*sql.DB{dbA, dbB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conns[i], openErrs[i] = db.Conn(ctx)
		}()
	}
	wg.Wait()
	for i := range conns {
		require.NoError(t, openErrs[i])
		conn := conns[i]
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
	}
	connA, connB := conns[0], conns[1]
	require.NotSame(t, a.se.GetUnderlyingEngine().Variables(), b.se.GetUnderlyingEngine().Variables())
	for _, conn := range []*sql.Conn{connA, connB} {
		require.NoError(t, conn.Raw(func(raw any) error {
			d := raw.(*DoltConn)
			require.Same(t, d.se.GetUnderlyingEngine().Variables(), d.gmsCtx.EngineVariables())
			return nil
		}))
	}

	defaultSortBuffer := getInt(connB, "SELECT @@session.sort_buffer_size")
	_, err := connA.ExecContext(ctx, "SET GLOBAL max_connections = 333")
	require.NoError(t, err)
	_, err = connB.ExecContext(ctx, "SET GLOBAL max_connections = 444")
	require.NoError(t, err)
	require.Equal(t, int64(333), getInt(connA, "SELECT @@global.max_connections"))
	require.Equal(t, int64(444), getInt(connB, "SELECT @@global.max_connections"))

	_, err = connA.ExecContext(ctx, "SET SESSION sort_buffer_size = 131072")
	require.NoError(t, err)
	require.Equal(t, int64(131072), getInt(connA, "SELECT @@session.sort_buffer_size"))
	require.Equal(t, defaultSortBuffer, getInt(connB, "SELECT @@session.sort_buffer_size"))

	connA2, err := dbA.Conn(ctx)
	require.NoError(t, err)
	defer connA2.Close()
	require.NoError(t, connA2.Raw(func(raw any) error {
		require.Same(t, a.se, raw.(*DoltConn).se)
		return nil
	}))
	require.Equal(t, int64(333), getInt(connA2, "SELECT @@global.max_connections"))
	require.Equal(t, defaultSortBuffer, getInt(connA2, "SELECT @@session.sort_buffer_size"))
}
