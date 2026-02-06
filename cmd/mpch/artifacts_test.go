package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteMeta_CollisionFailsUnlessOverwrite(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	runID := "testrun_1"
	meta := runMeta{
		RunID:     runID,
		CreatedAt: time.Now().UTC(),
		Plan:      planFields{DSN: "file:///tmp/dbs?commitname=a&commitemail=b@example.com"},
		Phases:    map[string]phaseMeta{},
		ExitCode:  0,
	}

	// First write should succeed.
	if err := writeMeta(runDir, runID, false, meta); err != nil {
		t.Fatalf("first writeMeta failed: %v", err)
	}

	// Second write without overwrite should fail.
	if err := writeMeta(runDir, runID, false, meta); err == nil {
		t.Fatalf("expected collision error, got nil")
	}

	// With overwrite it should succeed and meta.json should exist.
	if err := writeMeta(runDir, runID, true, meta); err != nil {
		t.Fatalf("overwrite writeMeta failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, runID, "meta.json")); err != nil {
		t.Fatalf("expected meta.json to exist: %v", err)
	}
}

func TestValidateRunID_AllowsSafeCharacters(t *testing.T) {
	t.Parallel()

	if err := validateRunID("abcDEF0123_-"); err != nil {
		t.Fatalf("expected valid run id, got: %v", err)
	}
	if err := validateRunID("bad/../id"); err == nil {
		t.Fatalf("expected invalid run id to fail")
	}
}

func TestWriteManifest_WritesFile(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	runID := "testrun_2"

	// Create run dir as writeManifest expects it to exist after meta write.
	if err := os.MkdirAll(filepath.Join(runDir, runID), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	manifest := newWorkerManifest(123, 2, 1)
	if err := writeManifest(runDir, runID, manifest); err != nil {
		t.Fatalf("writeManifest failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(runDir, runID, "manifest.json")); err != nil {
		t.Fatalf("expected manifest.json to exist: %v", err)
	}
}

