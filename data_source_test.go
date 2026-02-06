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
