package embedded

import (
	"errors"
	"strings"

	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/go-sql-driver/mysql"
)

// isRetryableEmbeddedErr returns true if |err| looks like transient contention that
// can succeed after waiting and reopening the engine (lock/readonly/manifest cases).
//
// This mirrors the intent of dolthubapi's dbutil retry classification, but is scoped
// to the embedded driver error shapes.
func isRetryableEmbeddedErr(err error) bool {
	if err == nil {
		return false
	}

	// Prefer explicit Dolt sentinel errors when available.
	if errors.Is(err, nbs.ErrDatabaseLocked) {
		return true
	}

	// Prefer structured MySQL errors when available.
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		// 1213 = ER_LOCK_DEADLOCK
		// Note: We keep this list intentionally small and conservative.
		if mysqlErr.Number == 1213 {
			return true
		}

		// Some Dolt errors use a generic/unknown error number but have a recognizable message.
		if isRetryableEmbeddedMsg(mysqlErr.Message) {
			return true
		}
	}

	return isRetryableEmbeddedMsg(err.Error())
}

func isRetryableEmbeddedMsg(msg string) bool {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return false
	}

	// Fail-fast journal lock timeout mode in Dolt.
	if strings.Contains(s, "database is locked by another dolt process") {
		return true
	}

	// Primary observed failure mode for concurrent embedded opens.
	if strings.Contains(s, "database is read only") {
		return true
	}
	// Common wrapper for the readonly case.
	if strings.Contains(s, "cannot update manifest") && strings.Contains(s, "read only") {
		return true
	}

	// Generic lock / contention signals (keep conservative to avoid retrying real user errors).
	if strings.Contains(s, "deadlock") {
		return true
	}
	if strings.Contains(s, "resource temporarily unavailable") {
		return true
	}
	if strings.Contains(s, "already locked") && strings.Contains(s, "lock") {
		return true
	}

	return false
}
