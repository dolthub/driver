package main

import (
	"context"
	"testing"
	"time"
)

func TestRunPhases_SetupFailureSkipsRunButAttemptsTeardown(t *testing.T) {
	t.Parallel()

	cfg := harnessConfig{
		DSN:             "file:///tmp/dbs?commitname=a&commitemail=b@example.com",
		Duration:        1 * time.Millisecond,
		SetupTimeout:    5 * time.Millisecond,
		RunTimeout:      1 * time.Second,
		TeardownTimeout: 1 * time.Second,
		SetupDelay:      50 * time.Millisecond,
		TeardownDelay:   0,
	}

	var events []string
	emit := func(phase, name string, _ any) {
		events = append(events, phase+":"+name)
	}

	code := runPhases(context.Background(), cfg, emit)
	if code == 0 {
		t.Fatalf("expected non-zero exit code")
	}

	for _, e := range events {
		if e == "run:phase_start" {
			t.Fatalf("expected no run events, saw %q; all=%v", e, events)
		}
	}

	var sawTeardown bool
	for _, e := range events {
		if e == "teardown:phase_start" {
			sawTeardown = true
			break
		}
	}
	if !sawTeardown {
		t.Fatalf("expected teardown to be attempted; events=%v", events)
	}
}

func TestRunPhases_RunFailureStillAttemptsTeardown(t *testing.T) {
	t.Parallel()

	cfg := harnessConfig{
		DSN:             "file:///tmp/dbs?commitname=a&commitemail=b@example.com",
		Duration:        50 * time.Millisecond,
		SetupTimeout:    1 * time.Second,
		RunTimeout:      5 * time.Millisecond,
		TeardownTimeout: 1 * time.Second,
		SetupDelay:      0,
		TeardownDelay:   0,
	}

	var events []string
	emit := func(phase, name string, _ any) {
		events = append(events, phase+":"+name)
	}

	code := runPhases(context.Background(), cfg, emit)
	if code == 0 {
		t.Fatalf("expected non-zero exit code")
	}

	want := []string{
		"setup:phase_start",
		"setup:phase_end",
		"run:phase_start",
		"run:phase_end",
		"teardown:phase_start",
		"teardown:phase_end",
	}
	if len(events) != len(want) {
		t.Fatalf("unexpected event count: got %d want %d; events=%v", len(events), len(want), events)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("unexpected event[%d]: got %q want %q; all=%v", i, events[i], want[i], events)
		}
	}
}

func TestRunPhases_TeardownFailureFailsOverall(t *testing.T) {
	t.Parallel()

	cfg := harnessConfig{
		DSN:             "file:///tmp/dbs?commitname=a&commitemail=b@example.com",
		Duration:        1 * time.Millisecond,
		SetupTimeout:    1 * time.Second,
		RunTimeout:      1 * time.Second,
		TeardownTimeout: 5 * time.Millisecond,
		SetupDelay:      0,
		TeardownDelay:   50 * time.Millisecond,
	}

	code := runPhases(context.Background(), cfg, func(string, string, any) {})
	if code == 0 {
		t.Fatalf("expected non-zero exit code")
	}
}

func TestRunPhases_CanceledContextStillAttemptsTeardown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := harnessConfig{
		DSN:             "file:///tmp/dbs?commitname=a&commitemail=b@example.com",
		Duration:        1 * time.Millisecond,
		SetupTimeout:    1 * time.Second,
		RunTimeout:      1 * time.Second,
		TeardownTimeout: 1 * time.Second,
	}

	var events []string
	emit := func(phase, name string, _ any) {
		events = append(events, phase+":"+name)
	}

	_ = runPhases(ctx, cfg, emit)

	var sawTeardown bool
	for _, e := range events {
		if e == "teardown:phase_start" {
			sawTeardown = true
			break
		}
	}
	if !sawTeardown {
		t.Fatalf("expected teardown to be attempted even when ctx canceled; events=%v", events)
	}
}

