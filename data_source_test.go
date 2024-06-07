package embedded

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDataSource(t *testing.T) {
	tests := []struct {
		name              string
		dsn               string
		expectedDirectory string
		expectedParams    map[string][]string
	}{
		{
			name:              "windows dsn test",
			dsn:               `file://C:\Users\RUNNER~1\AppData\Local\Temp\hostedapidb4128863379?commitname=Billy%20Batson&commitemail=shazam@gmail.com&database=hostedapidb`,
			expectedDirectory: `C:\Users\RUNNER~1\AppData\Local\Temp\hostedapidb4128863379`,
			expectedParams: map[string][]string{
				CommitNameParam:  {"Billy Batson"},
				CommitEmailParam: {"shazam@gmail.com"},
				DatabaseParam:    {"hostedapidb"},
			},
		},
		{
			name:              "unix dsn test",
			dsn:               `file:///Users/brian/datasets/test?commitname=Billy%20Batson&commitemail=shazam@gmail.com&database=hostedapidb&multiStatements=true&clientFoundRows=true`,
			expectedDirectory: `/Users/brian/datasets/test`,
			expectedParams: map[string][]string{
				CommitNameParam:      {"Billy Batson"},
				CommitEmailParam:     {"shazam@gmail.com"},
				DatabaseParam:        {"hostedapidb"},
				MultiStatementsParam: {"true"},
				ClientFoundRowsParam: {"true"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ds, err := ParseDataSource(test.dsn)
			require.NoError(t, err)
			require.Equal(t, test.expectedParams, ds.Params)
		})
	}
}
