package embedded

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// closeMultiRepoEnvDatabases closes any loaded DoltDB instances referenced by |mrEnv|.
// This is required in embedded mode when singleton caching is bypassed, since those
// stores are not tracked by dbfactory's process-global singleton map.
func closeMultiRepoEnvDatabases(mrEnv *env.MultiRepoEnv) {
	if mrEnv == nil {
		return
	}
	_ = mrEnv.Iter(func(_ string, dEnv *env.DoltEnv) (stop bool, err error) {
		if dEnv == nil {
			return false, nil
		}
		ddb := dEnv.DoltDB(context.Background())
		if ddb != nil {
			_ = ddb.Close()
		}
		return false, nil
	})
}

// singletonCacheKeysForMultiRepoEnv returns the dbfactory singleton cache keys for each DB in the MultiRepoEnv.
// Keys are based on the absolute on-disk path to the DB's ".dolt/noms" directory, converted to slash form.
// We also include the stats DB key (".dolt/stats/.dolt/noms") as a best-effort cleanup.
func singletonCacheKeysForMultiRepoEnv(mrEnv *env.MultiRepoEnv) []string {
	if mrEnv == nil {
		return nil
	}
	keys := make([]string, 0, 4)
	seen := make(map[string]struct{})

	_ = mrEnv.Iter(func(_ string, dEnv *env.DoltEnv) (stop bool, err error) {
		if dEnv == nil || dEnv.FS == nil {
			return false, nil
		}
		root, err := dEnv.FS.Abs("")
		if err != nil {
			return false, nil
		}

		nomsKey := filepath.ToSlash(filepath.Join(root, dbfactory.DoltDir, dbfactory.DataDir))
		if _, ok := seen[nomsKey]; !ok {
			seen[nomsKey] = struct{}{}
			keys = append(keys, nomsKey)
		}

		// Best-effort stats DB cleanup: "<dbroot>/.dolt/stats/.dolt/noms"
		statsKey := filepath.ToSlash(filepath.Join(root, dbfactory.DoltDir, dbfactory.StatsDir, dbfactory.DoltDir, dbfactory.DataDir))
		if _, ok := seen[statsKey]; !ok {
			seen[statsKey] = struct{}{}
			keys = append(keys, statsKey)
		}

		return false, nil
	})

	return keys
}

type singletonRefCounter struct {
	mu   sync.Mutex
	refs map[string]int
}

var globalSingletonRefCounts = &singletonRefCounter{
	refs: make(map[string]int),
}

func (s *singletonRefCounter) acquire(keys []string) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		s.refs[k]++
	}
}

func (s *singletonRefCounter) release(keys []string) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		n := s.refs[k]
		if n <= 1 {
			delete(s.refs, k)
			// Close and evict the singleton entry if it exists.
			_ = dbfactory.DeleteFromSingletonCache(k, true)
		} else {
			s.refs[k] = n - 1
		}
	}
}
