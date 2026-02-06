package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func newWorkerManifest(seed int64, readers, writers int) workerManifest {
	workers := make([]workerSpec, 0, readers+writers)

	for i := 0; i < writers; i++ {
		workers = append(workers, workerSpec{
			ID:    workerID(seed, roleWriter, i),
			Role:  roleWriter,
			Index: i,
		})
	}
	for i := 0; i < readers; i++ {
		workers = append(workers, workerSpec{
			ID:    workerID(seed, roleReader, i),
			Role:  roleReader,
			Index: i,
		})
	}

	return workerManifest{
		Seed:    seed,
		Readers: readers,
		Writers: writers,
		Workers: workers,
	}
}

func workerID(seed int64, role workerRole, index int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%d", seed, role, index)))
	// Short but stable. Enough uniqueness for a single manifest.
	return fmt.Sprintf("%s-%s", string(role)[0:1], hex.EncodeToString(sum[:4]))
}
