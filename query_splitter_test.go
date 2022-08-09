package embedded

import (
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

func TestQuerySplitter(t *testing.T) {
	tests := []struct {
		name    string
		queries []string
	}{
		{
			name:    "empty_test",
			queries: []string{},
		},
		{
			name:    "single_query_no_ending_semi",
			queries: []string{"SHOW TABLES"},
		},
		{
			name:    "single_query_with_ending_semi",
			queries: []string{"SHOW TABLES;"},
		},
		{
			name: "simple_multistatment",
			queries: []string{
				"SHOW DATABASES;",
				"USE db1;",
				"SHOW TABLES",
			},
		},
		{
			name: "escapes",
			queries: []string{
				`INSERT INTO t1 VALUES ("aoeu(\"",'aoeu(\'',` + "`aoeu(\\``);",
				`INSERT INTO t1 VALUES ("some other garbage",'more nonsense',` + "`again`);",
				`SELECT * FROM things join stuff on things.id = stuff.id;`,
				`SELECT * FROM (SELECT first, last FROM users where id = 3) as x join family on family.name = x.last`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newlineDelimQueries := strings.Join(test.queries, "\n")
			qs := NewQuerySplitter(newlineDelimQueries)

			for i := 0; ; i++ {
				query, err := qs.Next()

				if err == io.EOF {
					require.Equal(t, len(test.queries), i)
					break
				}

				require.NoError(t, err)
				require.Equal(t, test.queries[i], query)
			}
		})
	}
}

func split(newlineDelimStr string) []string {
	strs := strings.Split(newlineDelimStr, "\n")
	l := len(strs)

	if l > 0 && len(strs[l-1]) == 0 {
		strs = strs[:l-1]
	}

	return strs
}
