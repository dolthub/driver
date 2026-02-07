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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ParseDSN parses the provided DSN string into a Config suitable for NewConnector.
//
// It performs basic validation (required params, directory exists) and preserves
// the raw parsed param map (lower-cased keys) in Config.Params.
func ParseDSN(dsn string) (Config, error) {
	ds, err := ParseDataSource(dsn)
	if err != nil {
		return Config{}, err
	}

	var fs filesys.Filesys = filesys.LocalFS
	exists, isDir := fs.Exists(ds.Directory)
	if !exists {
		return Config{}, fmt.Errorf("'%s' does not exist", ds.Directory)
	} else if !isDir {
		return Config{}, fmt.Errorf("%s: is a file. need to specify a directory", ds.Directory)
	}

	name := ds.Params[CommitNameParam]
	if name == nil {
		return Config{}, fmt.Errorf("datasource %q must include the parameter %q", dsn, CommitNameParam)
	}
	if len(name) != 1 {
		return Config{}, fmt.Errorf("param %q must have exactly one value", CommitNameParam)
	}

	email := ds.Params[CommitEmailParam]
	if email == nil {
		return Config{}, fmt.Errorf("datasource %q must include the parameter %q", dsn, CommitEmailParam)
	}
	if len(email) != 1 {
		return Config{}, fmt.Errorf("param %q must have exactly one value", CommitEmailParam)
	}

	var database string
	if databases, ok := ds.Params[DatabaseParam]; ok {
		if len(databases) != 1 {
			return Config{}, fmt.Errorf("param %q must have exactly one value", DatabaseParam)
		}
		database = databases[0]
	}

	return Config{
		DSN:             dsn,
		Directory:       ds.Directory,
		CommitName:      name[0],
		CommitEmail:     email[0],
		Database:        database,
		MultiStatements: ds.ParamIsTrue(MultiStatementsParam),
		ClientFoundRows: ds.ParamIsTrue(ClientFoundRowsParam),
		Params:          ds.Params,
	}, nil
}
