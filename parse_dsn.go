package embedded

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ParseDSN parses the provided DSN string into a Config suitable for NewConnector.
//
// It performs basic validation (required params, directory exists) and preserves
// the raw parsed param map (lower-cased keys) in Config.Params.
//
// Note: legacy retry DSN params (open_retry*) are rejected. Configure retries via
// Config.BackOff instead.
func ParseDSN(dsn string) (Config, error) {
	ds, err := ParseDataSource(dsn)
	if err != nil {
		return Config{}, err
	}

	// Reject legacy retry params. These are now configured via Config.BackOff.
	for _, p := range []string{
		"open_retry",
		"open_retry_max_elapsed",
		"open_retry_initial",
		"open_retry_max_interval",
		"open_retry_max_tries",
	} {
		if _, ok := ds.Params[p]; ok {
			return Config{}, fmt.Errorf("%w (param %q in DSN)", ErrLegacyOpenRetryParamsUnsupported, p)
		}
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
