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

	enabled, err := parseRetryEnabled(ds)
	if err != nil {
		return RetryPolicy{}, err
	}
	p.Enabled = enabled

	if err := applyDurationParam(ds, RetryTimeoutParam, &p.Timeout); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyDurationParam(ds, RetryInitialDelayParam, &p.InitialDelay); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyDurationParam(ds, RetryMaxDelayParam, &p.MaxDelay); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyIntParam(ds, RetryMaxAttemptsParam, &p.MaxAttempts); err != nil {
		return RetryPolicy{}, err
	}

	if err := validateRetryPolicy(p); err != nil {
		return RetryPolicy{}, err
	}

	return p, nil
}

func parseRetryEnabled(ds *DoltDataSource) (bool, error) {
	if ds == nil {
		return false, nil
	}

	// Only "true" enables retry (matching ParamIsTrue behavior).
	if ds.ParamIsTrue(RetryParam) {
		return true, nil
	}

	// If explicitly set to something other than true (e.g. "false"), keep disabled,
	// but validate we can interpret it.
	vals, ok := ds.Params[RetryParam]
	if !ok || len(vals) != 1 {
		return false, nil
	}

	v := vals[0]
	if v != "false" && v != "0" && v != "no" && v != "off" && v != "true" && v != "1" && v != "yes" && v != "on" {
		return false, fmt.Errorf("invalid %s=%q (expected true|false)", RetryParam, v)
	}
	return false, nil
}

func applyDurationParam(ds *DoltDataSource, name string, dst *time.Duration) error {
	if ds == nil || dst == nil {
		return nil
	}
	d, ok, err := ds.paramDuration(name)
	if err != nil {
		return err
	}
	if ok {
		*dst = d
	}
	return nil
}

func applyIntParam(ds *DoltDataSource, name string, dst *int) error {
	if ds == nil || dst == nil {
		return nil
	}
	i, ok, err := ds.paramInt(name)
	if err != nil {
		return err
	}
	if ok {
		*dst = i
	}
	return nil
}

func validateRetryPolicy(p RetryPolicy) error {
	if p.Timeout < 0 {
		return fmt.Errorf("%s must be >= 0", RetryTimeoutParam)
	}
	if p.MaxAttempts < 0 {
		return fmt.Errorf("%s must be >= 0", RetryMaxAttemptsParam)
	}
	if p.InitialDelay < 0 {
		return fmt.Errorf("%s must be >= 0", RetryInitialDelayParam)
	}
	if p.MaxDelay < 0 {
		return fmt.Errorf("%s must be >= 0", RetryMaxDelayParam)
	}
	if p.InitialDelay > 0 && p.MaxDelay > 0 && p.InitialDelay > p.MaxDelay {
		return fmt.Errorf("%s (%v) must be <= %s (%v)", RetryInitialDelayParam, p.InitialDelay, RetryMaxDelayParam, p.MaxDelay)
	}
	if p.Enabled && p.MaxAttempts == 0 && p.Timeout == 0 {
		return fmt.Errorf("retry enabled but both %s and %s are 0", RetryMaxAttemptsParam, RetryTimeoutParam)
	}
	return nil
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

