package embedded

import (
	"fmt"
	"net/url"
)

type DoltDataSource struct {
	Directory string
	Params    map[string][]string
}

func ParseDataSource(dataSource string) (*DoltDataSource, error) {
	u, err := url.Parse(dataSource)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "file" {
		return nil, fmt.Errorf("datasource url '%s' must have a file url scheme", dataSource)
	}

	return &DoltDataSource{
		Directory: u.Path,
		Params:    u.Query(),
	}, nil
}
