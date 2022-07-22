package embedded

import (
	"fmt"
	"net/url"
	"strings"
)

// DoltDataSource provides access to the data provided by the connection string
type DoltDataSource struct {
	Directory string
	Params    map[string][]string
}

// ParseDataSource takes the connection string and parses out the parameters and the local filesys directory where the
// dolt database lives
func ParseDataSource(dataSource string) (*DoltDataSource, error) {
	u, err := url.Parse(dataSource)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "file" {
		return nil, fmt.Errorf("datasource url '%s' must have a file url scheme", dataSource)
	}

	queryParams := u.Query()
	lowerParams := make(map[string][]string, len(queryParams))
	for name, val := range queryParams {
		lowerParams[strings.ToLower(name)] = val
	}

	return &DoltDataSource{
		Directory: u.Path,
		Params:    lowerParams,
	}, nil
}
