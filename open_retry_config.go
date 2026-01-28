package embedded

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/dolthub/dolt/go/store/nbs"
)

type openRetryConfig struct {
	enabled     bool
	maxElapsed  time.Duration
	initial     time.Duration
	maxInterval time.Duration
	maxTries    int // 0 means unlimited (bounded by maxElapsed)
}

func defaultOpenRetryConfig() openRetryConfig {
	return openRetryConfig{
		enabled:     false,
		maxElapsed:  2 * time.Second,
		initial:     50 * time.Millisecond,
		maxInterval: 2 * time.Second,
		maxTries:    0,
	}
}

func parseBoolParam(ds *DoltDataSource, param string) (val bool, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return false, false, nil
	}
	// Treat presence without value as true.
	if len(values) == 1 && values[0] == "" {
		return true, true, nil
	}
	if len(values) != 1 {
		return false, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	switch v := values[0]; v {
	case "true", "TRUE", "True":
		return true, true, nil
	case "false", "FALSE", "False":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("param %q must be true or false, got %q", param, v)
	}
}

func parseDurationParam(ds *DoltDataSource, param string) (dur time.Duration, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	d, err := time.ParseDuration(values[0])
	if err != nil {
		return 0, true, fmt.Errorf("param %q must be a duration (e.g. 250ms, 2s), got %q: %w", param, values[0], err)
	}
	return d, true, nil
}

func parseIntParam(ds *DoltDataSource, param string) (n int, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	i, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, true, fmt.Errorf("param %q must be an integer, got %q: %w", param, values[0], err)
	}
	return i, true, nil
}

func parseOpenRetryConfig(ds *DoltDataSource) (openRetryConfig, error) {
	cfg := defaultOpenRetryConfig()

	if v, ok, err := parseBoolParam(ds, OpenRetryParam); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.enabled = v
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryMaxElapsed); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxElapsed = d
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryInitialBackoff); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.initial = d
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryMaxInterval); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxInterval = d
	}

	if n, ok, err := parseIntParam(ds, OpenRetryMaxTries); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxTries = n
	}

	if cfg.maxElapsed < 0 || cfg.initial < 0 || cfg.maxInterval < 0 {
		return openRetryConfig{}, fmt.Errorf("open retry durations must be non-negative")
	}
	if cfg.maxTries < 0 {
		return openRetryConfig{}, fmt.Errorf("param %q must be non-negative", OpenRetryMaxTries)
	}
	if cfg.maxInterval > 0 && cfg.initial > cfg.maxInterval {
		return openRetryConfig{}, fmt.Errorf("param %q must be <= %q", OpenRetryInitialBackoff, OpenRetryMaxInterval)
	}

	return cfg, nil
}

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
