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

	// Prepare dir first, then write meta.
	runPath, err := prepareRunDir(runDir, runID, false)
	if err != nil {
		t.Fatalf("prepareRunDir failed: %v", err)
	}
	if err := writeMeta(runPath, meta); err != nil {
		t.Fatalf("writeMeta failed: %v", err)
	}

	// Collision should fail without overwrite.
	if _, err := prepareRunDir(runDir, runID, false); err == nil {
		t.Fatalf("expected collision error, got nil")
	}

	// With overwrite it should succeed.
	if _, err := prepareRunDir(runDir, runID, true); err != nil {
		t.Fatalf("prepareRunDir overwrite failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, runID)); err != nil {
		t.Fatalf("expected run dir to exist: %v", err)
	}

	// Recompute runPath after overwrite and ensure meta write succeeds.
	runPath = filepath.Join(runDir, runID)
	if err := writeMeta(runPath, meta); err != nil {
		t.Fatalf("writeMeta after overwrite failed: %v", err)
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

	runPath, err := prepareRunDir(runDir, runID, false)
	if err != nil {
		t.Fatalf("prepareRunDir failed: %v", err)
	}

	manifest := newWorkerManifest(123, 2, 1)
	if err := writeManifest(runPath, manifest); err != nil {
		t.Fatalf("writeManifest failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(runDir, runID, "manifest.json")); err != nil {
		t.Fatalf("expected manifest.json to exist: %v", err)
	}
}
