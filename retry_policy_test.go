package embedded

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRetryPolicy_Defaults(t *testing.T) {
	ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb`)
	require.NoError(t, err)

	p, err := ParseRetryPolicy(ds)
	require.NoError(t, err)
	require.False(t, p.Enabled)
	require.Equal(t, 2*time.Second, p.Timeout)
	require.Equal(t, 10, p.MaxAttempts)
	require.Equal(t, 25*time.Millisecond, p.InitialDelay)
	require.Equal(t, 250*time.Millisecond, p.MaxDelay)
}

func TestParseRetryPolicy_Custom(t *testing.T) {
	ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb&retry=true&retrytimeout=5s&retrymaxattempts=42&retryinitialdelay=10ms&retrymaxdelay=200ms`)
	require.NoError(t, err)

	p, err := ParseRetryPolicy(ds)
	require.NoError(t, err)
	require.True(t, p.Enabled)
	require.Equal(t, 5*time.Second, p.Timeout)
	require.Equal(t, 42, p.MaxAttempts)
	require.Equal(t, 10*time.Millisecond, p.InitialDelay)
	require.Equal(t, 200*time.Millisecond, p.MaxDelay)
}

func TestParseRetryPolicy_Invalid(t *testing.T) {
	t.Run("bad duration", func(t *testing.T) {
		ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb&retrytimeout=nope`)
		require.NoError(t, err)
		_, err = ParseRetryPolicy(ds)
		require.Error(t, err)
	})
	t.Run("bad int", func(t *testing.T) {
		ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb&retrymaxattempts=wat`)
		require.NoError(t, err)
		_, err = ParseRetryPolicy(ds)
		require.Error(t, err)
	})
	t.Run("initial > max delay", func(t *testing.T) {
		ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb&retryinitialdelay=2s&retrymaxdelay=1s`)
		require.NoError(t, err)
		_, err = ParseRetryPolicy(ds)
		require.Error(t, err)
	})
	t.Run("retry enabled but no limits", func(t *testing.T) {
		ds, err := ParseDataSource(`file:///tmp/ds?commitname=a&commitemail=b&database=testdb&retry=true&retrytimeout=0s&retrymaxattempts=0`)
		require.NoError(t, err)
		_, err = ParseRetryPolicy(ds)
		require.Error(t, err)
	})
}

