package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func writeMeta(runPath string, meta runMeta) error {
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(runPath, "meta.json"), b, 0o644)
}

func writeManifest(runPath string, manifest workerManifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(runPath, "manifest.json"), b, 0o644)
}

func prepareRunDir(runDir, runID string, overwrite bool) (string, error) {
	runPath := filepath.Join(runDir, runID)
	if _, err := os.Stat(runPath); err == nil {
		if !overwrite {
			return "", fmt.Errorf("run directory already exists: %s (use --overwrite to replace)", runPath)
		}
		if err := os.RemoveAll(runPath); err != nil {
			return "", err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(runPath, "workers"), 0o755); err != nil {
		return "", err
	}
	return runPath, nil
}
