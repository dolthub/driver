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

	code, _ := runPhases(context.Background(), cfg, "test-run", "", emit)
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
		// Ignore auxiliary events; this test only cares about phase start/end ordering.
		if name != "phase_start" && name != "phase_end" {
			return
		}
		events = append(events, phase+":"+name)
	}

	code, _ := runPhases(context.Background(), cfg, "test-run", "", emit)
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

	code, _ := runPhases(context.Background(), cfg, "test-run", "", func(string, string, any) {})
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

	_, _ = runPhases(ctx, cfg, "test-run", "", emit)

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

func TestRunWithTicks_MonotonicAndBounded(t *testing.T) {
	t.Parallel()

	var ticks []tickFields
	emit := func(phase, name string, fields any) {
		if phase != "run" || name != "tick" {
			return
		}
		tf, ok := fields.(tickFields)
		if !ok {
			t.Fatalf("unexpected tick fields type: %T", fields)
		}
		ticks = append(ticks, tf)
	}

	dur := 30 * time.Millisecond
	interval := 5 * time.Millisecond
	err := runWithTicks(context.Background(), dur, interval, emit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Bounded: should not produce unbounded ticks.
	maxTicks := int(dur/interval) + 1
	if len(ticks) > maxTicks {
		t.Fatalf("too many ticks: got %d max %d; ticks=%v", len(ticks), maxTicks, ticks)
	}

	// Monotonic: count should strictly increase starting at 1.
	for i := range ticks {
		want := i + 1
		if ticks[i].Count != want {
			t.Fatalf("non-monotonic count at i=%d: got %d want %d; ticks=%v", i, ticks[i].Count, want, ticks)
		}
	}
}

func TestWorkerManifest_DeterministicIDs(t *testing.T) {
	t.Parallel()

	m1 := newWorkerManifest(123, 2, 1)
	m2 := newWorkerManifest(123, 2, 1)
	if len(m1.Workers) != 3 {
		t.Fatalf("unexpected worker count: %d", len(m1.Workers))
	}
	for i := range m1.Workers {
		if m1.Workers[i].ID != m2.Workers[i].ID {
			t.Fatalf("non-deterministic id at %d: %q vs %q", i, m1.Workers[i].ID, m2.Workers[i].ID)
		}
		if m1.Workers[i].Role != m2.Workers[i].Role || m1.Workers[i].Index != m2.Workers[i].Index {
			t.Fatalf("non-deterministic worker spec at %d", i)
		}
	}

	m3 := newWorkerManifest(124, 2, 1)
	// With a different seed, at least one ID should differ.
	same := 0
	for i := range m1.Workers {
		if m1.Workers[i].ID == m3.Workers[i].ID {
			same++
		}
	}
	if same == len(m1.Workers) {
		t.Fatalf("expected ids to vary with seed; m1=%v m3=%v", m1.Workers, m3.Workers)
	}
}
