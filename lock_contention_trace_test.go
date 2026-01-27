//go:build cgo

package embedded

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/dolthub/fslock"
	"github.com/stretchr/testify/require"
)

// TestInitThenOpenPing_RetryButLockStillHeld is a diagnostic regression test for embedded
// two-phase open flows:
//  1. init connection: CREATE DATABASE IF NOT EXISTS <db>
//  2. main connection: PingContext (triggers driver.Open)
//
// It logs which Dolt LOCK file(s) appear held after phase 1 and after phase 2.
// If phase 2 fails, the failure output includes the lock probe results.
//
// Run with:
//
//	go test -v ./... -run TestInitThenOpenPing_RetryButLockStillHeld -count=1
func TestInitThenOpenPing_RetryButLockStillHeld(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on windows due to differing process/file locking semantics")
	}
	if runTestsAgainstMySQL {
		t.Skip("this test is specific to embedded dolt driver filesystem locking")
	}

	tmpDir := t.TempDir()
	dbName := "testdb"

	initDSN := fmt.Sprintf(
		"file://%s?commitname=%s&commitemail=%s&multistatements=true&failonlocktimeout=true&nocache=true",
		encodeDir(tmpDir),
		"urlenc", // keep small + deterministic
		"urlenc@example.com",
	)
	// Enable retries for the main connection. Keep the timeout short so failures surface quickly.
	mainDSN := fmt.Sprintf(
		"file://%s?commitname=%s&commitemail=%s&multistatements=true&database=%s&failonlocktimeout=true&nocache=true&retry=true&retrytimeout=2s&retrymaxattempts=200&retryinitialdelay=10ms&retrymaxdelay=100ms",
		encodeDir(tmpDir),
		"urlenc",
		"urlenc@example.com",
		dbName,
	)

	ctx := context.Background()
	totalStart := time.Now()

	// Phase 1: init connection (create DB)
	initStart := time.Now()
	initDB, err := sqlOpen(t, initDSN)
	require.NoError(t, err)
	if _, err := initDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName); err != nil {
		_ = initDB.Close()
		t.Fatalf("init CREATE DATABASE failed: %v", err)
	}
	require.NoError(t, initDB.Close())
	t.Logf("init phase duration:  %s", time.Since(initStart))

	t.Log("lock probe after init close:")
	logLockProbe(t, tmpDir, dbName)

	// Phase 2: main connection (ping)
	mainStart := time.Now()
	mainDB, err := sqlOpen(t, mainDSN)
	require.NoError(t, err)
	pingErr := mainDB.PingContext(ctx)
	closeErr := mainDB.Close()
	t.Logf("main phase duration:  %s", time.Since(mainStart))
	t.Logf("total duration:       %s", time.Since(totalStart))

	t.Log("lock probe after main ping+close:")
	logLockProbe(t, tmpDir, dbName)

	if pingErr != nil {
		t.Fatalf("main PingContext failed (retries enabled): %v", pingErr)
	}
	require.NoError(t, closeErr)
}

// sqlOpen is a small wrapper so we can swap drivers if needed.
func sqlOpen(t *testing.T, dsn string) (*sql.DB, error) {
	t.Helper()
	return sql.Open(DoltDriverName, dsn)
}

func logLockProbe(t *testing.T, rootDir, dbName string) {
	t.Helper()

	// Candidate roots:
	// - multi-db directory root: <rootDir>
	// - created DB directory:    <rootDir>/<dbName>
	candidateRoots := []string{
		rootDir,
		filepath.Join(rootDir, dbName),
	}

	// Candidate lock paths under each root:
	// - main noms lock:  <root>/.dolt/noms/LOCK
	// - stats noms lock: <root>/.dolt/stats/.dolt/noms/LOCK
	for _, r := range candidateRoots {
		mainLock := filepath.Join(r, ".dolt", "noms", "LOCK")
		statsLock := filepath.Join(r, ".dolt", "stats", ".dolt", "noms", "LOCK")
		logOneLockProbe(t, mainLock)
		logOneLockProbe(t, statsLock)
	}
}

func logOneLockProbe(t *testing.T, lockPath string) {
	t.Helper()

	_, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Logf("lock missing: %s", lockPath)
			return
		}
		t.Logf("lock stat error: %s err=%v", lockPath, err)
		return
	}

	lck := fslock.New(lockPath)
	err = lck.LockWithTimeout(5 * time.Millisecond)
	if err == nil {
		_ = lck.Unlock()
		t.Logf("lock NOT held: %s", lockPath)
		return
	}
	if err == fslock.ErrTimeout {
		t.Logf("lock HELD: %s", lockPath)
		return
	}
	t.Logf("lock probe error: %s err=%v", lockPath, err)
}
