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

