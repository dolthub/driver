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
	"github.com/cenkalti/backoff/v4"
)

// Config configures an embedded Dolt SQL connector / driver.
//
// Typical usage is:
//   - Parse a DSN with ParseDSN (populates the DSN-derived fields)
//   - Optionally set BackOff to enable retries when opening the embedded engine
//   - Pass the config to NewConnector, then use sql.OpenDB(connector)
//
// DSN-derived fields are included so callers can parse once, adjust settings,
// and construct a connector without re-parsing string DSNs throughout the codebase.
type Config struct {
	// DSN is the original datasource name string used to create this config (optional).
	DSN string

	// Directory is the filesystem directory containing one or more Dolt databases (required).
	Directory string

	// CommitName and CommitEmail are used for Dolt commit metadata (required).
	CommitName  string
	CommitEmail string

	// Database is the initial database to connect to (optional).
	Database string

	// MultiStatements enables multi-statement support.
	MultiStatements bool

	// ClientFoundRows toggles the MySQL CLIENT_FOUND_ROWS capability in the session.
	ClientFoundRows bool

	// Params is the lower-cased DSN query param map as parsed from the DSN (optional).
	// This is preserved for forward-compat / feature flags while moving away from
	// DSN-driven configuration.
	Params map[string][]string

	// BackOff enables bounded retries when opening the embedded engine.
	//
	// If nil, engine open is attempted once.
	// If non-nil, NewConnector will retry opens for retryable errors using this BackOff.
	//
	// Note: BackOff implementations are stateful; callers should generally provide a
	// fresh instance per connector, and the connector will call Reset() before use.
	BackOff backoff.BackOff

	// Version is the Dolt environment version string used when loading repos (optional).
	// If empty, the connector will use a reasonable default.
	Version string
}
