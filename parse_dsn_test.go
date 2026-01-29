package embedded

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDSN_Basics(t *testing.T) {
	dir := t.TempDir()

	dsn := fmt.Sprintf(
		"file://%s?commitname=%s&commitemail=test@example.com&database=mydb&multistatements=true&clientfoundrows=true",
		dir,
		url.QueryEscape("Test User"),
	)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	require.Equal(t, dsn, cfg.DSN)
	require.Equal(t, dir, cfg.Directory)
	require.Equal(t, "Test User", cfg.CommitName)
	require.Equal(t, "test@example.com", cfg.CommitEmail)
	require.Equal(t, "mydb", cfg.Database)
	require.True(t, cfg.MultiStatements)
	require.True(t, cfg.ClientFoundRows)
	require.NotNil(t, cfg.Params)
}

func TestParseDSN_IsCaseInsensitiveForParamNames(t *testing.T) {
	dir := t.TempDir()

	// Mixed-case param names should parse and normalize.
	dsn := fmt.Sprintf("file://%s?CommitName=Test&CommitEmail=test@example.com&MultiStatements=true&ClientFoundRows=true", dir)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, "Test", cfg.CommitName)
	require.Equal(t, "test@example.com", cfg.CommitEmail)
	require.True(t, cfg.MultiStatements)
	require.True(t, cfg.ClientFoundRows)
}

func TestParseDSN_RequiresCommitNameAndEmail(t *testing.T) {
	dir := t.TempDir()

	_, err := ParseDSN(fmt.Sprintf("file://%s?commitemail=test@example.com", dir))
	require.Error(t, err)

	_, err = ParseDSN(fmt.Sprintf("file://%s?commitname=Test", dir))
	require.Error(t, err)
}

func TestParseDSN_ValidatesDirectoryExistsAndIsDir(t *testing.T) {
	dir := t.TempDir()

	// Missing directory.
	_, err := ParseDSN("file:///this/definitely/does/not/exist?commitname=Test&commitemail=test@example.com")
	require.Error(t, err)

	// Path is a file, not a directory.
	f := filepath.Join(dir, "not-a-dir")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))

	_, err = ParseDSN(fmt.Sprintf("file://%s?commitname=Test&commitemail=test@example.com", f))
	require.Error(t, err)
}
