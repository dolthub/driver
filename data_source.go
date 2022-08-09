package embedded

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	fileUrlPrefix    = "file://"
	fileUrlPrefixLen = len(fileUrlPrefix)
)

// DoltDataSource provides access to the data provided by the connection string
type DoltDataSource struct {
	Directory string
	Params    map[string][]string
}

// ParseDataSource takes the connection string and parses out the parameters and the local filesys directory where the
// dolt database lives
func ParseDataSource(dataSource string) (*DoltDataSource, error) {
	if !strings.HasPrefix(dataSource, fileUrlPrefix) {
		return nil, fmt.Errorf("datasource url '%s' must have a file url scheme", dataSource)
	}

	dataSource = dataSource[fileUrlPrefixLen:]
	paramsStart := strings.IndexRune(dataSource, '?')

	directory := dataSource
	params := make(map[string][]string)

	if paramsStart != -1 {
		directory = dataSource[:paramsStart]
		paramsStr := dataSource[paramsStart+1:]

		var err error
		params, err = url.ParseQuery(paramsStr)
		if err != nil {
			return nil, err
		}
	}

	lowerParams := make(map[string][]string, len(params))
	for name, val := range params {
		lowerParams[strings.ToLower(name)] = val
	}

	return &DoltDataSource{
		Directory: directory,
		Params:    lowerParams,
	}, nil
}

func (ds *DoltDataSource) ParamIsTrue(paramName string) bool {
	values, ok := ds.Params[paramName]
	return ok && len(values) == 1 && strings.ToLower(values[0]) == "true"
}
