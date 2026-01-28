package embedded

import (
	"errors"
	"os"

	"github.com/dolthub/dolt/go/store/nbs"
)

func isRetryableOpenErr(err error) bool {
	if err == nil {
		return false
	}
	// The intended primary retry signal for embedded usage: DB couldn't acquire its exclusive lock.
	if errors.Is(err, nbs.ErrDatabaseLocked) {
		return true
	}
	// Some filesystem / lower-level paths can surface timeouts as os.ErrDeadlineExceeded.
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	return false
}
