package embedded

import (
	"fmt"
	"strconv"
	"time"
)

// RetryPolicy configures retry behavior for transient embedded Dolt contention errors.
// This is parsed from DSN parameters. See constants in driver.go.
type RetryPolicy struct {
	Enabled      bool
	Timeout      time.Duration
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Enabled:      false,
		Timeout:      2 * time.Second,
		MaxAttempts:  10,
		InitialDelay: 25 * time.Millisecond,
		MaxDelay:     250 * time.Millisecond,
	}
}

func ParseRetryPolicy(ds *DoltDataSource) (RetryPolicy, error) {
	p := defaultRetryPolicy()
	if ds == nil {
		return p, nil
	}

	if ds.ParamIsTrue(RetryParam) {
		p.Enabled = true
	} else if vals, ok := ds.Params[RetryParam]; ok && len(vals) == 1 {
		// Explicitly set to something other than true (e.g. "false"). Keep disabled,
		// but validate we can interpret it.
		v := vals[0]
		if v != "false" && v != "0" && v != "no" && v != "off" && v != "true" && v != "1" && v != "yes" && v != "on" {
			return RetryPolicy{}, fmt.Errorf("invalid %s=%q (expected true|false)", RetryParam, v)
		}
	}

	if d, ok, err := ds.paramDuration(RetryTimeoutParam); err != nil {
		return RetryPolicy{}, err
	} else if ok {
		p.Timeout = d
	}
	if d, ok, err := ds.paramDuration(RetryInitialDelayParam); err != nil {
		return RetryPolicy{}, err
	} else if ok {
		p.InitialDelay = d
	}
	if d, ok, err := ds.paramDuration(RetryMaxDelayParam); err != nil {
		return RetryPolicy{}, err
	} else if ok {
		p.MaxDelay = d
	}
	if i, ok, err := ds.paramInt(RetryMaxAttemptsParam); err != nil {
		return RetryPolicy{}, err
	} else if ok {
		p.MaxAttempts = i
	}

	// Validation
	if p.Timeout < 0 {
		return RetryPolicy{}, fmt.Errorf("%s must be >= 0", RetryTimeoutParam)
	}
	if p.MaxAttempts < 0 {
		return RetryPolicy{}, fmt.Errorf("%s must be >= 0", RetryMaxAttemptsParam)
	}
	if p.InitialDelay < 0 {
		return RetryPolicy{}, fmt.Errorf("%s must be >= 0", RetryInitialDelayParam)
	}
	if p.MaxDelay < 0 {
		return RetryPolicy{}, fmt.Errorf("%s must be >= 0", RetryMaxDelayParam)
	}
	if p.InitialDelay > 0 && p.MaxDelay > 0 && p.InitialDelay > p.MaxDelay {
		return RetryPolicy{}, fmt.Errorf("%s (%v) must be <= %s (%v)", RetryInitialDelayParam, p.InitialDelay, RetryMaxDelayParam, p.MaxDelay)
	}
	if p.Enabled && p.MaxAttempts == 0 && p.Timeout == 0 {
		return RetryPolicy{}, fmt.Errorf("retry enabled but both %s and %s are 0", RetryMaxAttemptsParam, RetryTimeoutParam)
	}

	return p, nil
}

func (ds *DoltDataSource) paramDuration(name string) (time.Duration, bool, error) {
	vals, ok := ds.Params[name]
	if !ok || len(vals) == 0 {
		return 0, false, nil
	}
	if len(vals) != 1 {
		return 0, false, fmt.Errorf("invalid %s: expected single value", name)
	}
	d, err := time.ParseDuration(vals[0])
	if err != nil {
		return 0, false, fmt.Errorf("invalid %s=%q: %w", name, vals[0], err)
	}
	return d, true, nil
}

func (ds *DoltDataSource) paramInt(name string) (int, bool, error) {
	vals, ok := ds.Params[name]
	if !ok || len(vals) == 0 {
		return 0, false, nil
	}
	if len(vals) != 1 {
		return 0, false, fmt.Errorf("invalid %s: expected single value", name)
	}
	i, err := strconv.Atoi(vals[0])
	if err != nil {
		return 0, false, fmt.Errorf("invalid %s=%q: %w", name, vals[0], err)
	}
	return i, true, nil
}

