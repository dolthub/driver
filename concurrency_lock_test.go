package embedded

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const helperEnvKey = "DOLT_DRIVER_HELPER"

// TestHelperProcess_HoldOpen is executed in a separate process to keep an embedded engine open
// for a specified duration. This is used to reliably reproduce inter-process contention.
func TestHelperProcess_HoldOpen(t *testing.T) {
	if os.Getenv(helperEnvKey) != "1" {
		t.Skip("helper process only")
	}

	dsn := os.Getenv("DOLT_DRIVER_HELPER_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "missing DOLT_DRIVER_HELPER_DSN")
		os.Exit(2)
	}
	hold := os.Getenv("DOLT_DRIVER_HELPER_HOLD_MS")
	if hold == "" {
		fmt.Fprintln(os.Stderr, "missing DOLT_DRIVER_HELPER_HOLD_MS")
		os.Exit(2)
	}
	holdMs, err := strconv.Atoi(hold)
	if err != nil || holdMs < 0 {
		fmt.Fprintf(os.Stderr, "invalid DOLT_DRIVER_HELPER_HOLD_MS=%q\n", hold)
		os.Exit(2)
	}

	ctx := context.Background()
	db, err := sql.Open(DoltDriverName, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERR: sql.Open: %v\n", err)
		os.Exit(1)
	}
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ERR: Ping: %v\n", err)
		os.Exit(1)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERR: Conn: %v\n", err)
		os.Exit(1)
	}
	_, _ = conn.ExecContext(ctx, "use testdb")

	fmt.Println("READY")
	time.Sleep(time.Duration(holdMs) * time.Millisecond)
	// Hard-exit to avoid any potential hangs in close paths while reproducing contention.
	os.Exit(0)
}

// TestHelperProcess_AttemptWrite is executed in a separate process. It attempts to open the
// embedded Dolt database and perform a write, then reports whether it succeeded.
func TestHelperProcess_AttemptWrite(t *testing.T) {
	if os.Getenv(helperEnvKey) != "1" {
		t.Skip("helper process only")
	}

	dsn := os.Getenv("DOLT_DRIVER_HELPER_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "missing DOLT_DRIVER_HELPER_DSN")
		os.Exit(2)
	}

	ctx := context.Background()
	db, err := sql.Open(DoltDriverName, dsn)
	if err != nil {
		fmt.Printf("ERR: sql.Open: %v\n", err)
		os.Exit(0)
	}
	defer db.Close()

	// Ping may fail if the database can't be opened due to another process holding the lock.
	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("ERR: Ping: %v\n", err)
		os.Exit(0)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("ERR: Conn: %v\n", err)
		os.Exit(0)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "use testdb")
	if _, err := conn.ExecContext(ctx, "insert into lock_test values (2)"); err != nil {
		fmt.Printf("ERR: INSERT: %v\n", err)
		os.Exit(0)
	}

	// If we got here, we were able to write concurrently.
	fmt.Println("OK")
	os.Exit(1)
}

// TestHelperProcess_WriteOnce is executed in a separate process and performs a single write attempt.
// Unlike TestHelperProcess_AttemptWrite, it exits 0 on success and 1 on failure so the parent can
// use the exit status naturally.
func TestHelperProcess_WriteOnce(t *testing.T) {
	if os.Getenv(helperEnvKey) != "1" {
		t.Skip("helper process only")
	}

	dsn := os.Getenv("DOLT_DRIVER_HELPER_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "missing DOLT_DRIVER_HELPER_DSN")
		os.Exit(2)
	}

	ctx := context.Background()
	db, err := sql.Open(DoltDriverName, dsn)
	if err != nil {
		fmt.Printf("ERR: sql.Open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("ERR: Ping: %v\n", err)
		os.Exit(1)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("ERR: Conn: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	_, _ = conn.ExecContext(ctx, "use testdb")
	if _, err := conn.ExecContext(ctx, "insert into lock_test values (2)"); err != nil {
		fmt.Printf("ERR: INSERT: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
	os.Exit(0)
}

// TestHelperProcess_InitSchema initializes a testdb and lock_test table, then exits.
func TestHelperProcess_InitSchema(t *testing.T) {
	if os.Getenv(helperEnvKey) != "1" {
		t.Skip("helper process only")
	}
	dsn := os.Getenv("DOLT_DRIVER_HELPER_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "missing DOLT_DRIVER_HELPER_DSN")
		os.Exit(2)
	}

	ctx := context.Background()
	db, err := sql.Open(DoltDriverName, dsn)
	if err != nil {
		fmt.Printf("ERR: sql.Open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("ERR: Ping: %v\n", err)
		os.Exit(1)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("ERR: Conn: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "drop database if exists testdb"); err != nil {
		fmt.Printf("ERR: drop db: %v\n", err)
		os.Exit(1)
	}
	if _, err := conn.ExecContext(ctx, "create database testdb"); err != nil {
		fmt.Printf("ERR: create db: %v\n", err)
		os.Exit(1)
	}
	if _, err := conn.ExecContext(ctx, "use testdb"); err != nil {
		fmt.Printf("ERR: use: %v\n", err)
		os.Exit(1)
	}
	if _, err := conn.ExecContext(ctx, "create table lock_test (pk int primary key)"); err != nil {
		fmt.Printf("ERR: create table: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
	os.Exit(0)
}

func TestConcurrentOpenWhileWriterHoldsLock_FailsWithoutRetry(t *testing.T) {
	if runTestsAgainstMySQL {
		t.Skip("this test is specific to embedded dolt driver filesystem locking")
	}

	tmpDir := t.TempDir()

	// Build DSN for a multi-db directory (same pattern as smoke_test.go).
	query := url.Values{
		"commitname":      []string{"Lock Tester"},
		"commitemail":     []string{"lock@test.invalid"},
		"database":        []string{"testdb"},
		"multistatements": []string{"true"},
		// Make contention deterministic: fail fast instead of opening read-only.
		"failonlocktimeout": []string{"true"},
		"nocache":           []string{"true"},
	}
	dsn := url.URL{Scheme: "file", Path: encodeDir(tmpDir), RawQuery: query.Encode()}

	ctx := context.Background()

	// Initialize db + table in the parent process.
	db, err := sql.Open(DoltDriverName, dsn.String())
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PingContext(ctx))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "drop database if exists testdb")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create database testdb")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "use testdb")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table lock_test (pk int primary key)")
	require.NoError(t, err)

	// Keep the parent connection open to simulate a writer holding the DB open.
	// This matches the real-world scenario where one process keeps an embedded engine alive.

	helperCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(helperCtx, os.Args[0], "-test.run=TestHelperProcess_AttemptWrite") // #nosec G204 -- test binary
	cmd.Env = append(os.Environ(),
		helperEnvKey+"=1",
		"DOLT_DRIVER_HELPER_DSN="+dsn.String(),
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())

	// Read helper output (it should exit quickly with an ERR line if it can't write).
	lines := []string{}
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	_ = stdout.Close()

	// Wait for process exit and capture stderr if needed.
	waitErr := cmd.Wait()
	elapsed := time.Since(start)
	helperStderr, _ := ioReadAllNoErr(stderr)

	// We expect the helper to fail its write attempt quickly without retrying.
	require.Less(t, elapsed, 1500*time.Millisecond, "expected no long retry/wait; helper took %v", elapsed)

	out := strings.Join(lines, "\n")
	if strings.Contains(out, "OK") {
		t.Fatalf("expected helper to fail write due to lock contention, but it wrote successfully.\nstdout:\n%s\nstderr:\n%s", out, string(helperStderr))
	}
	require.Contains(t, out, "ERR:", "expected helper to report an error.\nstdout:\n%s\nstderr:\n%s\nwaitErr:%v", out, string(helperStderr), waitErr)

	lower := strings.ToLower(out + "\n" + string(helperStderr))
	require.True(t,
		strings.Contains(lower, "read only") ||
			strings.Contains(lower, "lock") ||
			strings.Contains(lower, "temporarily unavailable") ||
			strings.Contains(lower, "already locked") ||
			strings.Contains(lower, "locked by another dolt process"),
		"unexpected error for lock contention.\nstdout:\n%s\nstderr:\n%s\nwaitErr:%v", out, string(helperStderr), waitErr,
	)
}

func TestConcurrentWrite_WaitsAndEventuallySucceeds_WithDriverRetries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on windows due to differing process/file locking semantics")
	}
	if runTestsAgainstMySQL {
		t.Skip("this test is specific to embedded dolt driver filesystem locking")
	}

	tmpDir := t.TempDir()
	// Enable retries in the helper process so it can wait for the writer to release.
	query := url.Values{
		"commitname":      []string{"Retry Tester"},
		"commitemail":     []string{"retry@test.invalid"},
		"database":        []string{"testdb"},
		"multistatements": []string{"true"},
		"retry":           []string{"true"},
		"retrytimeout":    []string{"5s"},
		"retrymaxattempts": []string{"100"},
		"retryinitialdelay": []string{"25ms"},
		"retrymaxdelay":     []string{"250ms"},
		// Ensure we fail-fast on lock contention and always open a fresh local DB instance.
		"failonlocktimeout": []string{"true"},
		"nocache":           []string{"true"},
	}
	dsn := url.URL{Scheme: "file", Path: encodeDir(tmpDir), RawQuery: query.Encode()}

	// Initialize schema in a separate process so this test process does not keep any embedded
	// engine/lock state alive while we exercise contention behavior.
	initCmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess_InitSchema") // #nosec G204 -- test binary
	initCmd.Env = append(os.Environ(),
		helperEnvKey+"=1",
		"DOLT_DRIVER_HELPER_DSN="+dsn.String(),
	)
	outInit, err := initCmd.CombinedOutput()
	require.NoErrorf(t, err, "schema init failed:\n%s", strings.TrimSpace(string(outInit)))

	// Hold the DB open in a separate process to ensure we reproduce the inter-process lock.
	holdMs := 750
	holderCtx, holderCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer holderCancel()

	holderCmd := exec.CommandContext(holderCtx, os.Args[0], "-test.run=TestHelperProcess_HoldOpen") // #nosec G204 -- test binary
	holderCmd.Env = append(os.Environ(),
		helperEnvKey+"=1",
		"DOLT_DRIVER_HELPER_DSN="+dsn.String(),
		"DOLT_DRIVER_HELPER_HOLD_MS="+strconv.Itoa(holdMs),
	)
	holderStdout, err := holderCmd.StdoutPipe()
	require.NoError(t, err)
	holderStderr, err := holderCmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, holderCmd.Start())

	// Wait for holder to be ready.
	readyCh := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(holderStdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "READY" {
				close(readyCh)
				return
			}
		}
	}()
	select {
	case <-readyCh:
	case <-time.After(2 * time.Second):
		b, _ := ioReadAllNoErr(holderStderr)
		t.Fatalf("holder did not become ready; stderr:\n%s", string(b))
	}

	// Ensure the holder definitely terminates so the lock is released.
	time.AfterFunc(time.Duration(holdMs)*time.Millisecond, func() {
		if holderCmd.Process != nil {
			_ = holderCmd.Process.Kill()
		}
	})

	helperCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(helperCtx, os.Args[0], "-test.run=TestHelperProcess_WriteOnce") // #nosec G204 -- test binary
	cmd.Env = append(os.Environ(),
		helperEnvKey+"=1",
		"DOLT_DRIVER_HELPER_DSN="+dsn.String(),
	)
	outBytes, err := cmd.CombinedOutput()
	elapsed := time.Since(start)
	out := strings.TrimSpace(string(outBytes))

	// Ensure holder exits.
	_ = holderCmd.Wait()

	// With retries, we expect it to succeed eventually (exit 0) and to have waited at least releaseAfter.
	require.NoErrorf(t, err, "helper failed (expected to succeed once retries are implemented).\nElapsed: %v\nOutput:\n%s", elapsed, out)
	require.Contains(t, out, "OK", "expected helper to report success; got:\n%s", out)
	require.GreaterOrEqual(t, elapsed, time.Duration(holdMs)*time.Millisecond, "expected helper to wait for lock release; elapsed=%v output=%q", elapsed, out)
}

func ioReadAllNoErr(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return b, nil
}

